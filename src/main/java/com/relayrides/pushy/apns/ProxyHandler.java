package com.relayrides.pushy.apns;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>Verifies HTTP proxy connection before any data are written or read. Closes channel if connection fails.
 * <p>Must be registered as <b>first</b> in the pipeline.</p>
 *
 * @author Michal Dvorak
 * @since 09/06/14
 */
public class ProxyHandler extends ChannelDuplexHandler {

	private static final Charset HTTP_CHARSET = Charset.forName("ASCII7");
	private static final Pattern STATUS_PATTERN = Pattern.compile("^\\S+\\s+(\\d+)\\s");

	private static final Logger log = LoggerFactory.getLogger(ProxyHandler.class);

	private final LazyChannelPromise proxyPromise = new LazyChannelPromise();
	private final SocketAddress proxyAddress;
	private InetSocketAddress remoteAddress;
	private long connectTimeoutMillis = 10000;

	private EventExecutor executor;
	private boolean connected;

	public ProxyHandler(final SocketAddress proxyAddress) {
		assert proxyAddress != null : "proxyAddress";
		this.proxyAddress = proxyAddress;
	}

	public InetSocketAddress remoteAddress() {
		return remoteAddress;
	}

	public Future<Channel> proxyFuture() {
		return proxyPromise;
	}

	public long getConnectTimeoutMillis() {
		return connectTimeoutMillis;
	}

	public void setConnectTimeoutMillis(long handshakeTimeout, TimeUnit unit) {
		if (unit == null) {
			throw new NullPointerException("unit");
		}

		setConnectTimeoutMillis(unit.toMillis(handshakeTimeout));
	}

	public void setConnectTimeoutMillis(long connectTimeoutMillis) {
		if (connectTimeoutMillis < 0) {
			throw new IllegalArgumentException("handshakeTimeoutMillis: " + connectTimeoutMillis + " (expected: >= 0)");
		}

		this.connectTimeoutMillis = connectTimeoutMillis;
	}

	@Override
	public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
		if (ctx.channel().isActive()) {
			// Channel is opened already, which means we can not override channel address
			ctx.close();
			throw new ProxyConnectionException("Channel already initialized, unable to redirect it thru proxy server");
		}

		// Init
		assert executor == null : "Cannot use same handler twice";
		executor = ctx.executor();
	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		executor = null;
	}

	@Override
	public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelPromise promise) throws Exception {
		// Validate address type
		if (!(remoteAddress instanceof InetSocketAddress)) {
			// Report failure
			promise.setFailure(new IllegalArgumentException("Only remote InetSocketAddress is supported in proxied channel"));
			return;
		}

		// Store for later
		this.remoteAddress = (InetSocketAddress) remoteAddress;

		// Use own promise
		final ChannelPromise connectPromise = ctx.newPromise();
		connectPromise.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				// Initiate proxy tunnel
				doProxyConnect(ctx);
			}
		});

		// On proxy success, forward result to original promise
		proxyPromise.addListener(new GenericFutureListener<Future<? super Channel>>() {
			@Override
			public void operationComplete(Future<? super Channel> future) throws Exception {
				// Notify listeners
				if (future.isSuccess()) {
					if (log.isTraceEnabled()) {
						final InetSocketAddress remoteAddress = remoteAddress();
						log.trace("Successfully opened proxy tunnel to " + remoteAddress.getHostString() + ':' + remoteAddress.getPort());
					}

					promise.setSuccess();
				} else {
					log.debug("Failed to establish proxy tunnel, closing channel", future.cause());
					promise.setFailure(future.cause());
				}
			}
		});

		// Connect to the proxy instead
		ctx.connect(proxyAddress, localAddress, connectPromise);
	}

	protected void doProxyConnect(final ChannelHandlerContext ctx) {
		// Schedule timeout
		final ScheduledFuture<?> timeoutFuture;
		if (connectTimeoutMillis > 0) {
			timeoutFuture = ctx.executor().schedule(new Runnable() {
				@Override
				public void run() {
					if (!proxyPromise.isDone()) {
						proxyPromise.setFailure(new ProxyConnectionException("Connection timed out"));
					}
				}
			}, connectTimeoutMillis, TimeUnit.MILLISECONDS);
		} else {
			timeoutFuture = null;
		}

		proxyPromise.addListener(new GenericFutureListener<Future<Channel>>() {
			@Override
			public void operationComplete(Future<Channel> f) throws Exception {
				if (timeoutFuture != null) {
					timeoutFuture.cancel(false);
				}
			}
		});

		// Send CONNECT command
		final String header = "CONNECT " + remoteAddress.getHostString() + ':' + remoteAddress.getPort() + " HTTP/1.0\r\n\r\n";
		final ByteBuf data = Unpooled.copiedBuffer(header, HTTP_CHARSET);

		// Write data
		final Channel.Unsafe unsafe = ctx.channel().unsafe();
		unsafe.write(data, ctx.channel().voidPromise());
		unsafe.flush();
	}

	@Override
	public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
		if (connected) {
			// Continue in normal operation
			ctx.fireChannelRead(msg);
		} else if (msg instanceof ByteBuf) {
			// Read proxy response first
			final ByteBuf data = (ByteBuf) msg;

			// This code is inspired by http://code.google.com/p/javapns/
			int newlines = 0;
			int statusEnd = -1;

			// Look for empty line
			while (newlines < 2) {
				if (data.readableBytes() < 1) {
					data.release();
					proxyPromise.setFailure(new ProxyConnectionException("Unexpected EOF from proxy, fragmented response is not supported"));
					return;
				}

				// Read byte by byte
				int c = data.readByte();
				if (c == '\n') {
					if (statusEnd < 0) statusEnd = data.readerIndex();
					newlines++;
				} else if (c != '\r') {
					newlines = 0;
				}
			}

			// Check status
			final String statusLine = data.slice(0, statusEnd).toString(HTTP_CHARSET);
			final Matcher statusMatcher = STATUS_PATTERN.matcher(statusLine);
			if (!statusMatcher.find()) {
				data.release();
				proxyPromise.setFailure(new ProxyConnectionException("Failed to parse HTTP response: " + statusLine.trim()));
				return;
			}

			// Must be 2xx code
			final int statusCode = Integer.valueOf(statusMatcher.group(1));
			connected = statusCode >= 200 && statusCode < 300;

			if (!connected) {
				data.release();
				proxyPromise.setFailure(new ProxyConnectionException("Failed to establish proxy tunnel: " + statusLine.trim()));
				return;
			}

			// Success
			if (!proxyPromise.trySuccess(ctx.channel())) {
				// Channel is closed in default listener
				data.release();
				return;
			}

			// Continue reading
			if (data.readableBytes() > 0) {
				// Pass along only remaining data
				ctx.fireChannelRead(data.slice());
			}
		} else {
			// Close if needed
			ReferenceCountUtil.release(msg);

			// Error
			proxyPromise.setFailure(new ProxyConnectionException("Expected ByteBuf object, other types are not supported. Is there any other handler before ProxyHandler? It must be first."));
		}
	}

	private final class LazyChannelPromise extends DefaultPromise<Channel> {

		@Override
		protected EventExecutor executor() {
			if (executor == null) {
				throw new IllegalStateException();
			}
			return executor;
		}
	}
}
