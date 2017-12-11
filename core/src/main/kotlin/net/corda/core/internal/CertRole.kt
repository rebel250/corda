package net.corda.core.identity

import net.corda.core.CordaOID
import org.bouncycastle.asn1.*
import java.security.cert.Certificate
import java.security.cert.X509Certificate

/**
 * Describes the Corda role a certificate is used for. This is used both to verify the hierarchy of certificates is
 * correct, and to determine which is the well known identity's certificate.
 */
// The order of the entries in the enum MUST NOT be changed, as their ordinality is used as an identifier.
// TODO: Link to the specification once it has a permanent URL
enum class CertRole(val parent: CertRole?, val isIdentity: Boolean) : ASN1Encodable {
    /**
     * A Doorman (intermediate CA of some kind).
     */
    INTERMEDIATE_CA(null, false),
    /** Signing key for the network map */
    NETWORK_MAP(null, false),
    /** The node level CA from which the TLS and well known identity certificates are issued. */
    NODE_CA(INTERMEDIATE_CA, false),
    /** The transport layer security certificate */
    TLS(NODE_CA, false),
    /** A well known (publicly visible) identity */
    WELL_KNOWN_IDENTITY(NODE_CA, true),
    /** A confidential (limited visibility) identity */
    CONFIDENTIAL_IDENTITY(WELL_KNOWN_IDENTITY, true);

    companion object {
        fun getInstance(obj: ASN1Encodable): CertRole {
            val ordinal = (obj as ASN1Integer).positiveValue
            return CertRole.values().get(ordinal.toInt() - 1)
        }
        fun getInstance(data: ByteArray): CertRole? {
            val sequence = ASN1Sequence.getInstance(data)
            require(sequence.size() == 1)
            return getInstance(sequence.getObjectAt(0))
        }

        fun extract(cert: Certificate): CertRole? {
            val x509Cert = cert as? X509Certificate
            return if (x509Cert != null) {
                extract(x509Cert)
            } else {
                null
            }
        }

        fun extract(cert: X509Certificate): CertRole? {
            val extensionData: ByteArray? = cert.getExtensionValue(CordaOID.X509_EXTENSION_CORDA_ROLE)
            return if (extensionData != null) {
                val extensionString = DEROctetString.getInstance(extensionData)
                getInstance(extensionString.octets)
            } else {
                null
            }
        }
    }

    override fun toASN1Primitive(): ASN1Primitive = ASN1Integer(this.ordinal + 1L)
}