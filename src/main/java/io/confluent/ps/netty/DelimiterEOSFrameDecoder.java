package io.confluent.ps.netty;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;

public class DelimiterEOSFrameDecoder extends DelimiterBasedFrameDecoder {
    public DelimiterEOSFrameDecoder(int maxFrameLength, ByteBuf delimiter) {
        super(maxFrameLength, delimiter);
    }

    public DelimiterEOSFrameDecoder(
        int maxFrameLength, boolean stripDelimiter, ByteBuf delimiter)
    {
            super(maxFrameLength, stripDelimiter, delimiter);
    }

    public DelimiterEOSFrameDecoder(
        int maxFrameLength, boolean stripDelimiter, boolean failFast,
        ByteBuf delimiter)
    {
        super(maxFrameLength, stripDelimiter, failFast, delimiter);
    }

    public DelimiterEOSFrameDecoder(int maxFrameLength, ByteBuf... delimiters) {
        super(maxFrameLength, delimiters);
    }

    public DelimiterEOSFrameDecoder(
            int maxFrameLength, boolean stripDelimiter, ByteBuf... delimiters)
    {
        super(maxFrameLength, stripDelimiter, delimiters);
    }
    public DelimiterEOSFrameDecoder(
            int maxFrameLength, boolean stripDelimiter, boolean failFast, ByteBuf... delimiters)
    {
        super(maxFrameLength, stripDelimiter, failFast, delimiters);
    }

}
