. ec2_variables.sh
ec2-run-instances ami-57360a20 -t t1.micro --region eu-west-1 -k $key -g me2  
sleep 10;
IP=$(ec2-describe-instances|awk '{ if($1=="PRIVATEIPADDRESS") { print $4 } }')
echo $IP

