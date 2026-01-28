package io.netty.handler.ssl;

import java.io.File;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.NoSuchPaddingException;

/**
 * Proxy the SslContext class to allow running protected methods
 */
public abstract class AbstractSslContextProxy extends SslContext {

    public static X509Certificate[] _toX509Certificates(File file) throws CertificateException {
        return SslContext.toX509Certificates(file);
    }

    public static PrivateKey _toPrivateKey(File keyFile, String keyPassword) throws NoSuchAlgorithmException,
                                                                NoSuchPaddingException, InvalidKeySpecException,
                                                                InvalidAlgorithmParameterException,
                                                                KeyException, IOException
    {
        return SslContext.toPrivateKey(keyFile, keyPassword);
    }

}
