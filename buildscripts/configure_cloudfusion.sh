cp cloudfusion/config/Sugarsync.ini cloudfusion/config/sugarsync_testing.ini
cp cloudfusion/config/Dropbox.ini cloudfusion/config/Dropbox.ini.bck #backup config file
cp cloudfusion/config/Google.ini cloudfusion/config/Google_testing.ini
cp cloudfusion/config/AmazonS3.ini cloudfusion/config/AmazonS3_testing.ini

perl -pi -e "s/user = /user = ${USR}/g" cloudfusion/config/Dropbox.ini
perl -pi -e "s/password = /password = ${PW}/g" cloudfusion/config/Dropbox.ini
perl -pi -e "s/user = /user = ${USR}/g" cloudfusion/config/sugarsync_testing.ini
perl -pi -e "s/password = /password = ${PW}/g" cloudfusion/config/sugarsync_testing.ini
perl -pi -e "s/client_id =.*/client_id =${GS_ID}/g" cloudfusion/config/Google_testing.ini
perl -pi -e "s/client_secret =.*/client_secret =${GS_KEY}/g" cloudfusion/config/Google_testing.ini
perl -pi -e "s/access_key_id =.*/access_key_id =${S3_ID}/g" cloudfusion/config/AmazonS3_testing.ini
perl -pi -e "s/secret_access_key =.*/secret_access_key =${S3_KEY}/g" cloudfusion/config/AmazonS3_testing.ini
perl -pi -e "s/user =.*/user =${WEBDAV_USR}/g" cloudfusion/config/Webdav_tonline_testing.ini
perl -pi -e "s/password =.*/password =${WEBDAV_PWD}/g" cloudfusion/config/Webdav_tonline_testing.ini
perl -pi -e "s/user =.*/user =${WEBDAV2_USR}/g" cloudfusion/config/Webdav_gmx_testing.ini
perl -pi -e "s/password =.*/password =${WEBDAV2_PWD}/g" cloudfusion/config/Webdav_gmx_testing.ini
perl -pi -e "s/user =.*/user =${WEBDAV3_USR}/g" cloudfusion/config/Webdav_box_testing.ini
perl -pi -e "s/password =.*/password =${WEBDAV3_PWD}/g" cloudfusion/config/Webdav_box_testing.ini
perl -pi -e "s/user =.*/user =${WEBDAV4_USR}/g" cloudfusion/config/Webdav_yandex_testing.ini
perl -pi -e "s/password =.*/password =${WEBDAV4_PWD}/g" cloudfusion/config/Webdav_yandex_testing.ini

perl -pi -e "s/bucket_name =.*/bucket_name = cloudfusion42/g" cloudfusion/config/Google_testing.ini
perl -pi -e "s/bucket_name =.*/bucket_name = cloudfusion42/g" cloudfusion/config/AmazonS3_testing.ini
