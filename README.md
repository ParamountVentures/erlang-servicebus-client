myapp
=====

A demo of using Erlang to talk to Azure Service Bus via AMQP 1.0 using the RabbitMQ client.

As this uses a beta version of the MQ library it includes the binaries due to some complexities in building it.

Build
=====
Create a Service Bus instance on Azure and queue with Session and Partitioning enabled.

$ git clone git@github.com:ParamountVentures/erlang-servicebus-client.git

$ cd erlang-servicebus-client

Add the config details for Azure to src/myapp.app.src - placeholders are highlighted.

$ ./rebar3 shell

$ application:start(myapp).

Send a message to Service Bus:

$ myapp_app:send().

Retrieve the message:

$ myapp_app:retrieve().

