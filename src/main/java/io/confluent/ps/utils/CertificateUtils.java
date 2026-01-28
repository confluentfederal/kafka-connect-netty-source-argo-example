package io.confluent.ps.utils;

import java.io.FileInputStream;
import java.nio.file.AccessMode;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class CertificateUtils {
    private CertificateUtils() {
    }

    /**
     * Instantiate a keystore and load it with the passed in file.
     *
     * @param location path to the keystore file
     * @param type     of keystore, such as PKCS12 or JKS
     * @param password used to access the keystore
     * @return an instantiated keystore
     * @throws KeyStoreAccessException   if the location cannot be accessed
     * @throws KeyStoreTypeException     if a keystore cannot be initialized
     * @throws KeyStorePasswordException if the keystore cannot be loaded with the
     *                                   file
     */
    public static KeyStore loadKeyStore(String location, String type, char[] password)
            throws KeyStoreAccessException, KeyStoreTypeException, InvalidPasswordException {
        Path keyStoreLocation = Paths.get(location);

        // Validate the path
        try {
            FileSystems.getDefault().provider().checkAccess(
                    keyStoreLocation,
                    AccessMode.READ);
        } catch (Exception ex) {
            throw new KeyStoreAccessException(ex);
        }

        // Instantiate a keystore for the given type
        KeyStore keyStore;
        try {
            keyStore = KeyStore.getInstance(type);
        } catch (KeyStoreException ex) {
            throw new KeyStoreTypeException(ex);
        }

        // Provide location of Java Keystore and password for access
        try {
            keyStore.load(new FileInputStream(keyStoreLocation.toFile()), password);
        } catch (Exception ex) {
            throw new InvalidPasswordException(ex);
        }

        return keyStore;
    }

    /**
     * Validate the alias is a key entry or if the alias is blank then retrieve the
     * alias of the first key entry.
     *
     * @param keyStore containing the key entries
     * @param alias    optional alias name of the intended key entry
     * @return the validated alias of intended key entry
     * @throws KeyStoreEmptyException if no aliases exist
     * @throws InvalidAliasException  if the alias is not a key entry
     */
    public static String getAliasOrFirstKeyEntry(KeyStore keyStore, String alias)
            throws KeyStoreEmptyException, InvalidAliasException {
        String keyAlias = null;
        List<String> aliases = null;
        try {
            aliases = Collections.list(keyStore.aliases());

            log.debug("KeyStore aliases: {}", String.join(", ", aliases));

            if (aliases.isEmpty()) {
                throw new Exception("No aliases found in the keystore");
            }
        } catch (Exception ex) {
            throw new KeyStoreEmptyException(ex);
        }

        try {
            if (StringUtils.isBlank(alias)) {
                keyAlias = aliases.stream()
                        .filter(a -> {
                            try {
                                return keyStore.isKeyEntry(a);
                            } catch (KeyStoreException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .findFirst()
                        .orElseThrow(() -> new GeneralSecurityException(
                                "Keystore doen't contain a certificate with a private key"));
            } else {
                // Check the provided alias is a key entry
                if (!keyStore.isKeyEntry(keyAlias)) {
                    throw new KeyStoreException("The alias '" + keyAlias + "' is not a key entry");
                }
            }
        } catch (Exception ex) {
            throw new InvalidAliasException(ex);
        }

        return keyAlias;
    }

    /**
     * Retrieves the certificate chain associated with the alias and ensures the
     * entries are X.509 certificate entries.
     *
     * @param keyStore to retrieve the certificate chain from
     * @param alias    associated with the certificate chain
     * @return iterable X509 certificate chain
     * @throws KeyStoreException if any of the certificates are not X.509
     *                           certificates
     */
    public static Iterable<? extends X509Certificate> getCertificateChain(KeyStore keyStore, String alias)
            throws KeyStoreException {
        ArrayList<X509Certificate> chain = new ArrayList<>();
        Certificate[] certs = keyStore.getCertificateChain(alias);
        for (Certificate cert : certs) {
            if (!(cert instanceof X509Certificate)) {
                throw new KeyStoreException("Certificate is not an X.509 certificate");
            }
            chain.add((X509Certificate) cert);
        }

        return chain;
    }

    /**
     * Retrieves the key entry associated with the alias and ensures it is a Private
     * Key Entry.
     *
     * @param keyStore to retrieve the key entry from
     * @param alias    assocaited with the key entry
     * @param password that unlocks the key entry
     * @return private key entry
     * @throws InvalidPasswordException if the password does not unlock the key
     *                                  entry
     * @throws KeyStoreException        if the key entry is not a private key entry
     */
    public static PrivateKey getPrivateKey(KeyStore keyStore, String alias, char[] password)
            throws InvalidPasswordException, KeyStoreException {
        Key key = null;
        try {
            key = keyStore.getKey(alias, password);
        } catch (Exception ex) {
            throw new InvalidPasswordException(ex);
        }

        if (!(key instanceof PrivateKey)) {
            throw new KeyStoreException("Key entry is not a Private KEy Entry");
        }

        return (PrivateKey) key;
    }

    public static class KeyStoreAccessException extends Exception {
        public KeyStoreAccessException(Exception cause) {
            super(cause);
        }
    }

    public static class KeyStoreTypeException extends Exception {
        public KeyStoreTypeException(Exception cause) {
            super(cause);
        }
    }

    public static class KeyStoreEmptyException extends Exception {
        public KeyStoreEmptyException(Exception cause) {
            super(cause);
        }
    }

    public static class InvalidAliasException extends Exception {
        public InvalidAliasException(Exception cause) {
            super(cause);
        }
    }

    public static class InvalidPasswordException extends Exception {
        public InvalidPasswordException(Exception cause) {
            super(cause);
        }
    }

}
