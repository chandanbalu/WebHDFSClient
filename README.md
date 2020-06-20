## Web HDFS Client
=================================

WebHDFS is a Restful endpoint that allows you to interact with HDFS directly.

You can use a multitude of clients with webHDFS, the challenge tends to be getting authentication working due to the client needing to negotiate kerberos on your behalf. It is possible to generate a token which can be used in place of kerberos if your client can't provide kerberos forwarding.

webHDFS resource: https://hadoop.apache.org/docs/r1.0.4/webhdfs.html

### Curl Commands
- List a Directory : Submit a HTTPs GET request

    `curl -k  -i  -u <DS_ID>:<PASSWORD>  "https://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"`

- Get Home Directory : Submit a HTTPs GET request

    `curl -k  -i  -u <DS_ID>:<PASSWORD>  "https://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETHOMEDIRECTORY"`

- Create and Write to a File: Submit a HTTPs PUT request

    `curl -k -i -u -X PUT <DS_ID>:<PASSWORD>"http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE&overwrite=true`

- Download a File: Submit a HTTPs GET request

    `curl -k -i -u <DS_ID>:<PASSWORD>"http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN`

- Delete a File: Submit a HTTPs DELETE request

    `curl -k -i -u -X DELETE <DS_ID>:<PASSWORD>"http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=DELETE`

## Java/Scala Client for WebHDFS REST API

WebHDFS provides a simple ,standard way to execute Hadoop file system operations by an external client that does not necessarily run on the Hadoop cluster itself. The requirement for WebHDFS is that the client needs to have a direct connection to namenode and datanodes via the predefined ports.

On Windows:
Standlone API
Upload a file to HDFS

java -cp C:\Users\c795701\WebHDFSClient\target\WebHDFSClient-1.0-SNAPSHOT-jar-with-dependencies.jar com.cargill.hdfsclient.UploadWebHDFSClient --host <HOSTNAME> --port 8443 --username <DS_ID> --password  <PASSWORD> --hdfsfile /user/<DS_ID>/test_dir/titanic.csv --localfile
C:\\Users\\c795701\\WebHDFSClient\\src\\main\\resources\\titanic_copy.csv

Download a file from HDFS to Local Filesystem

java -cp C:\Users\c795701\WebHDFSClient\target\WebHDFSClient-1.0-SNAPSHOT-jar-with-dependencies.jar com.cargill.hdfsclient.DownloadWebHDFSClient --host <HOSTNAME> --port 8443 --username <DS_ID> --password  <PASSWORD> --hdfsfile /user/<DS_ID>/test_dir/titanic.csv --localfile
C:\\Users\\c795701\\WebHDFSClient\\src\\main\\resources\\titanic_copy.csv

List a HDFS File/Directory

java -cp C:\Users\c795701\WebHDFSClient\target\WebHDFSClient-1.0-SNAPSHOT-jar-with-dependencies.jar com.cargill.hdfsclient.DIRStatusWebHDFSClient --host <HOSTNAME> --port 8443 --username <DS_ID> --password  <PASSWORD> --hdfsfile /user/<DS_ID>/test_dir/titanic.csv

Delete a HDFS File/Directory

java -cp C:\Users\c795701\WebHDFSClient\target\WebHDFSClient-1.0-SNAPSHOT-jar-with-dependencies.jar com.cargill.hdfsclient.DeleteDIRWebHDFSClient --host <HOSTNAME> --port 8443 --username <DS_ID> --password  <PASSWORD> --hdfsfile /user/<DS_ID>/test_dir/titanic.csv

Bundled API
Download a file from HDFS to Local Filesystem

java -cp C:\Users\c795701\WebHDFSClient\target\WebHDFSClient-1.0-SNAPSHOT-jar-with-dependencies.jar com.cargill.hdfsclient.HDFSClient --operation download --hdfsfile /user/<DS_ID>/test_dir/titanic.csv --localfile C:\\Users\\c795701\\WebHDFSClient\\src\\main\\resources\\titanic_copy.csv

Upload a file to HDFS

java -cp C:\Users\c795701\WebHDFSClient\target\WebHDFSClient-1.0-SNAPSHOT-jar-with-dependencies.jar com.cargill.hdfsclient.HDFSClient --operation upload --hdfsfile /user/<DS_ID>/test_dir/titanic.csv --localfile C:\\Users\\c795701\\WebHDFSClient\\src\\main\\resources\\titanic_copy.csv

Create a HDFS File/Directory

java -cp C:\Users\c795701\WebHDFSClient\target\WebHDFSClient-1.0-SNAPSHOT-jar-with-dependencies.jar com.cargill.hdfsclient.HDFSClient --operation create --hdfsfile /user/<DS_ID>/test_dir2

List a HDFS File/Directory

java -cp C:\Users\c795701\WebHDFSClient\target\WebHDFSClient-1.0-SNAPSHOT-jar-with-dependencies.jar com.cargill.hdfsclient.HDFSClient --operation list --hdfsfile /user/<DS_ID>/test_dir/titanic.csv

Delete a HDFS File/Directory

java -cp C:\Users\c795701\WebHDFSClient\target\WebHDFSClient-1.0-SNAPSHOT-jar-with-dependencies.jar com.cargill.hdfsclient.HDFSClient --operation delete --hdfsfile /user/<DS_ID>/test_dir2

On Unix or Linux platforms:

Replace the backslash with forwardslash in the path. 
