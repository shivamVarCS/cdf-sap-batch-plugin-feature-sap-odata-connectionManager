package com.google.cloud.datafusion.plugin.sap.odata.source.transport;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.datafusion.plugin.util.GCSPath;
import com.google.cloud.datafusion.plugin.util.Util;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.UUID;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

/**
 *
 */

public class SapX509Manager {
  private static final Logger LOGGER = LoggerFactory.getLogger(SapX509Manager.class);

  private static final String AUTO_DETECT = "auto-detect";

  private Path localCertPath;

  private final String x509EncodedString;
  private final String gcsCertPath;
  private final String gcpProjectId;
  private final String certPassphrase;

  public SapX509Manager(String gcpProjectId, String gcsCertPath, String certPassphrase) {
    this(null, gcpProjectId, gcsCertPath, certPassphrase);
  }

  public SapX509Manager(String x509EncodedString, String certPassphrase) {
    this(x509EncodedString, null, null, certPassphrase);
  }

  private SapX509Manager(String x509EncodedString, String gcpProjectId, String gcsCertPath, String certPassphrase) {
    this.x509EncodedString = x509EncodedString;
    this.gcpProjectId = gcpProjectId;
    this.gcsCertPath = gcsCertPath;
    this.certPassphrase = certPassphrase;
  }

  public void configureSSLLayer(OkHttpClient.Builder builder)
    throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {

    //check if SSL configuration is required based on the provided parameters at the constructor.
    if (Util.isNullOrEmpty(x509EncodedString)
      && (Util.isNullOrEmpty(gcsCertPath)
      || Util.isNullOrEmpty(gcpProjectId))) {
      LOGGER.debug("SSL configuration is not required.");
      return;
    }

    final KeyStore trustStore;
    try (ByteArrayInputStream cert = getX509AsStream()) {
      trustStore = KeyStore.getInstance("PKCS12");
      trustStore.load(cert, certPassphrase.toCharArray());
    }

    TrustManagerFactory trustManagerFactory = TrustManagerFactory
      .getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);

    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
    if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
      throw new IllegalStateException("Unexpected default trust managers: " + Arrays.toString(trustManagers));
    }

    SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
    sslContext.init(null, trustManagers, null);

    X509TrustManager trustManager = (X509TrustManager) trustManagers[0];
    builder.sslSocketFactory(sslContext.getSocketFactory(), trustManager);
  }

  public String getX509AsBase64EncodedString() throws IOException {
    //check if SSL configuration is required based on the provided parameters at the constructor.
    if (Util.isNullOrEmpty(x509EncodedString) && (Util.isNullOrEmpty(gcsCertPath) ||
      Util.isNullOrEmpty(gcpProjectId))) {
      LOGGER.debug("SSL configuration is not required.");
      return "";
    }

    localCertPath = downloadAndGetLocalCertPath();
    byte[] cert = Files.readAllBytes(localCertPath);

    cleanUpDownloadedResource();

    return Base64.getEncoder().encodeToString(cert);
  }

  private ByteArrayInputStream getX509AsStream() throws IOException {
    if (Util.isNotNullOrEmpty(x509EncodedString)) {
      return new ByteArrayInputStream(Base64.getDecoder().decode(x509EncodedString));
    }

    localCertPath = downloadAndGetLocalCertPath();
    byte[] cert = Files.readAllBytes(localCertPath);

    cleanUpDownloadedResource();

    return new ByteArrayInputStream(cert);
  }

  private Path downloadAndGetLocalCertPath() throws IOException {

    GCSPath gcsPathObj = GCSPath.from(gcsCertPath);
    String bucketName = gcsPathObj.getBucket();
    String bucketFilePath = gcsPathObj.getName();

    StorageOptions options = StorageOptions.newBuilder()
      .setProjectId(getProjectId(gcpProjectId))
      .setCredentials(GoogleCredentials.getApplicationDefault())
      .build();

    Storage storage = options.getService();
    Blob x509Blob = storage.get(bucketName, bucketFilePath);

    if (x509Blob == null) {
      throw new IllegalArgumentException(String.format(
        "SAP X509 certificated (%s) is missing. Please make sure the required X509 certificate is"
          + " uploaded to your specified Google Cloud Storage bucket '%s'.", gcsCertPath, bucketName));
    }

    Path localTempDirPath = Files.createTempDirectory("_sap.cert");
    Path path = Paths.get(localTempDirPath.toString(), UUID.randomUUID().toString());
    x509Blob.downloadTo(path);

    return path;
  }

  private void cleanUpDownloadedResource() {
//    try {
//      FileUtils.forceDelete(localCertPath.getParent().toFile());
//    } catch (IOException ioe) {
//      LOGGER.warn("Unable to delete downloaded content at path '{}'", localCertPath, ioe);
//    }

    try (Stream<Path> pathStream = Files.walk(localCertPath.getParent())) {
      pathStream
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(File::delete);
    } catch (IOException ioe) {
      LOGGER.warn("Unable to delete downloaded content at path '{}'", localCertPath, ioe);
    }
  }

  /**
   * Used to return the default GCP 'Project Id' in case the given string is 'auto-detect'
   * otherwise returns the given string.
   *
   * @param gcpProjectId contains the actual GCP project id. Default value: auto-detect.
   * @return in case of 'auto-detect default GCP 'Project Id' otherwise same value is returned
   */
  private static String getProjectId(String gcpProjectId) {
    if (AUTO_DETECT.equals(gcpProjectId)) {
      return ServiceOptions.getDefaultProjectId();
    }
    return gcpProjectId;
  }
}
