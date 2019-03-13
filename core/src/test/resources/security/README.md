### SSL Test Keystores and Truststores

This directory contains a test keystore and truststore for the Cassandra client and Server respectively to test SSL communication. 

Createad based on instructions from: 
https://docs.datastax.com/en/archived/cassandra/2.0/cassandra/security/secureSslEncryptionTOC.html

#### Manual instructions
Keystores used in tests were generated with these commands. This really only needs to be done once, the certificates expires ≈100 years into the future. 

The steps are documented here for future reference in case they need to be regenerated. Reason for having obtuse passwords is to have something that mimics a complex production setup and to minimize programming errors based for the ssl setup.

##### Key stores
Generate server-to-server keystore
```
keytool -genkey -keyalg RSA -alias sts_cassandra_test -keystore sts_keystore.jks -storepass LZk1rh3AUnWm3g -validity 36000 -keysize 4096 -dname "CN=Server to Server,OU=Akka Cassandra, O=Akka Cassandra, L=Cassandratown, ST=Cassandrastate, C=CA"

Enter key password for <sts_cassandra_test>
  (RETURN if same as keystore password): *<Press return>*
```

Generate server-to-client keystore
```
keytool -genkey -keyalg RSA -alias stc_cassandra_test -keystore stc_keystore.jks -storepass 8yJQLUnGkwZxOw -validity 36000 -keysize 4096 -dname "CN=Server to Client,OU=Akka Cassandra, O=Akka Cassandra, L=Cassandratown, ST=Cassandrastate, C=CA"

Enter key password for <sts_cassandra_test>
  (RETURN if same as keystore password): *<Press return>*
```

Generate client-to-server keystore
```
keytool -genkey -keyalg RSA -alias cts_cassandra_test -keystore cts_keystore.jks -storepass 5zsGJ0LxnpozNQ -validity 36000 -keysize 4096 -dname "CN=Client to Server,OU=Akka Cassandra, O=Akka Cassandra, L=Cassandratown, ST=Cassandrastate, C=CA"

Enter key password for <sts_cassandra_test>
  (RETURN if same as keystore password): *<Press return>*
```

##### Trust stores
Export certificates
```
keytool -export -alias sts_cassandra_test -file sts_cassandra_test.crt -keystore sts_keystore.jks -storepass LZk1rh3AUnWm3g
keytool -export -alias stc_cassandra_test -file stc_cassandra_test.crt -keystore stc_keystore.jks -storepass 8yJQLUnGkwZxOw
keytool -export -alias cts_cassandra_test -file cts_cassandra_test.crt -keystore cts_keystore.jks -storepass 5zsGJ0LxnpozNQ
```

Create trust stores
```
# Server-to-server trust store (trusts it's own certificate, only one node)
keytool -import -trustcacerts -alias sts_cassandra_test -file sts_cassandra_test.crt -keystore sts_truststore.jks -storepass uRq1zJnwDgLAT2
Trust this certificate? [no]:  *yes*

# Server-to-client trust store (trusts client-to-server certificate)
keytool -import -trustcacerts -alias cts_cassandra_test -file cts_cassandra_test.crt -keystore stc_truststore.jks -storepass erZHDS9Eo0CcNo
Trust this certificate? [no]:  *yes*

# Client-to-server trust store (trusts server-to-client certificate)
keytool -import -trustcacerts -alias stc_cassandra_test -file stc_cassandra_test.crt -keystore cts_truststore.jks -storepass hbbUtqn3Y1D4Tw
Trust this certificate? [no]:  *yes*

```

Clean up certificate files
```
rm sts_cassandra_test.crt
rm stc_cassandra_test.crt
rm cts_cassandra_test.crt
```
 
##### Verify everything looks OK
```
# Server-to-server
keytool -list -v -keystore sts_keystore.jks -storepass LZk1rh3AUnWm3g
keytool -list -v -keystore sts_truststore.jks -storepass uRq1zJnwDgLAT2

# Server-to-client
keytool -list -v -keystore stc_keystore.jks -storepass 8yJQLUnGkwZxOw
keytool -list -v -keystore stc_truststore.jks -storepass erZHDS9Eo0CcNo

# Client-to-server
keytool -list -v -keystore cts_keystore.jks -storepass 5zsGJ0LxnpozNQ
keytool -list -v -keystore cts_truststore.jks -storepass hbbUtqn3Y1D4Tw 
```
You are now done and ready to run the tests!

##### Troubleshooting
To verify that the Certificates and SSL connection is setup all right it is helpful to use the openssl toolchain.

First step is to get key and certificates out of JKS-files into PKCS12-files.
```
# Server-to-server
keytool -importkeystore -srckeystore sts_keystore.jks -destkeystore sts_keystore.p12 -deststoretype PKCS12 -srcalias sts_cassandra_test -srcstorepass LZk1rh3AUnWm3g -deststorepass LZk1rh3AUnWm3g -destkeypass LZk1rh3AUnWm3g

# Server-to-client
keytool -importkeystore -srckeystore stc_keystore.jks -destkeystore stc_keystore.p12 -deststoretype PKCS12 -srcalias stc_cassandra_test -srcstorepass 8yJQLUnGkwZxOw -deststorepass 8yJQLUnGkwZxOw -destkeypass 8yJQLUnGkwZxOw

# Client-to-server
keytool -importkeystore -srckeystore cts_keystore.jks -destkeystore cts_keystore.p12 -deststoretype PKCS12 -srcalias cts_cassandra_test -srcstorepass 5zsGJ0LxnpozNQ -deststorepass 5zsGJ0LxnpozNQ -destkeypass 5zsGJ0LxnpozNQ
```

Second step is to extract both key-files and certificate files for client to server. Preferably in PEM-format as that is the default working format for OpenSSL
```
openssl pkcs12 -in cts_keystore.p12 -nokeys -out cts_cassandra_test.crt.pem
>Enter Import Password: 5zsGJ0LxnpozNQ
openssl pkcs12 -in cts_keystore.p12 -nodes -nocerts -out cts_cassandra_test.key.pem
>Enter Import Password: 5zsGJ0LxnpozNQ
```

A useful way to troubleshoot is to start one of the SSL-tests create a break point in the test and then use openssl s_client to verify TLS connectivity.

The port number changes for each test. To find out which port number is used you need to set cassandra server logging to DEBUG-level in the `logback-test.xml`-file. 
```
openssl s_client \
	-connect 127.0.0.1:<TEST_PORT_NR> \
	-cert cts_cassandra_test.crt.pem \
	-key cts_cassandra_test.key.pem \
	-state
```

Another option is to use CQLSH to connect to the server.

#### Server-to-server stores
| Property | Value |
| ---- | --- | 
| Cert alias | sts_cassadra_test |
| Keystore Password | LZk1rh3AUnWm3g |
| Truststore Password | uRq1zJnwDgLAT2 |

#### Server-to-client stores
| Property | Value |
| ---- | --- | 
| Cert alias | stc_cassandra_test |
| Keystore Password | 8yJQLUnGkwZxOw |
| Truststore Password | erZHDS9Eo0CcNo |

#### Client-to-server stores
| Property | Value |
| ---- | --- | 
| Cert alias | cassandra_client_test |
| Keystore Password | 5zsGJ0LxnpozNQ |
| Truststore Password | hbbUtqn3Y1D4Tw |
