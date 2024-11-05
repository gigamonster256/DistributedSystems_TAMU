### Instructions to install RabbitMQ on Ubuntu 22.04

- `chmod +x install_rabbit.sh`
- Run the script `./install_rabbit.sh`

  <hr>

### NOTE: In the installation logs in the console, you will see some messages like:
`E: Failed to fetch ...` or `E: Some index files failed to download ...`
#### You can safely ignore these errors.

<hr>

### OPTIONAL: Once installed successfully, you can use the commands below to check out the current status of your rabbitmq

- `systemctl status rabbitmq-server`
- `sudo rabbitmqctl list_users`
  - Should now see guest as admin
- `sudo rabbitmqctl list_vhosts`
  - Should see /
- `sudo rabbitmqctl list_permissions -p /`
  - Should see list of permissions for /

<hr>

### NOTE: Once you successfully execute the installation script, every time your VM is powered on, the RabbitMQ cluster starts automatically. You do not need to manually do anything.

<hr>

### Now that installation was successful, to test it, you can compile and run the two test C++ Files we have provided:

- Compile using: `g++ -o send send.cpp -lrabbitmq &&
g++ -o receive receive.cpp -lrabbitmq`
- Next, in one terminal, run: `./receive`
- In another terminal, run: `./send`


<hr>

### NOTE: 
You can visit the RabbitMQ Management Plugin UI at http://localhost:15672/ on your Virtual Machine and login with the credentials: 

Username: guest

Password: guest

The management UI is very useful and is implemented as a single page application which relies on the HTTP API. Some of the features include: Declare, list and delete exchanges, queues, bindings, users, virtual hosts and user permissions.