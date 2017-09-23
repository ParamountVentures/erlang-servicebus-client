%%%-------------------------------------------------------------------
%% @doc myapp public API
%% @end
%%%-------------------------------------------------------------------

-module(myapp_app).

-include_lib("/rabbitmq-amqp1.0-client/src/amqp10_client.hrl").

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, send/0, retrieve/0]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
  io:format("Starting ..."),
  amqp10_client_sup:start_link(),
  myapp_sup:start_link().

send() ->
  io:format("Connecting ...\n"),
	
  {ok, Address} = application:get_env(myapp, "Address"),
  {ok, Hostname} = application:get_env(myapp, "Hostname"),
  {ok, Port} = application:get_env(myapp, "Port"),
  {ok, User} = application:get_env(myapp, "User"),
  {ok, Password} = application:get_env(myapp, "Password"),
  {ok, Container} = application:get_env(myapp, "Queue"),
  {ok, Queue} = application:get_env(myapp, "Queue"),
  OpnConf = #{
	      address => Address,
	      hostname => Hostname,
              port => Port,
	      notify => self(),
	      tls_opts => {secure_port, [{versions, ['tlsv1.1']}]},
              container_id => Container,
              sasl => {plain, User, Password}
	    },

  {ok, Connection} = amqp10_client:open_connection(OpnConf),

  receive
    {amqp10_event, {connection, Connection, opened}} ->
    io:format("Connected ...\n")
  after 2000 ->
    exit(connection_timeout)
  end,  

  {ok, Session} = amqp10_client:begin_session(Connection),
  receive
    {amqp10_event, {session, Session, begun}} ->
    	io:format("Session Id ...\n")
  after 2000 ->
    exit(session_timeout)
  end,
  {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                  <<"test-sender">>,
                                                  Queue), 
    % wait for credit to be received
    receive
      {amqp10_event, {link, Sender, credited}} -> 
        ok
    after 2000 ->
      exit(credited_timeout)
    end,
  
    % send message
    Msg0 = amqp10_msg:new(<<"test">>, <<"my-body">>, false),
    P =#{group_id => <<"test">>},
    Msg1 = amqp10_msg:set_properties(P, Msg0),
    Q =#{"x-opt-partition-key" => 12345},
    Msg = amqp10_msg:set_message_annotations(Q, Msg1),
    ok = amqp10_client:send_msg(Sender, Msg),
    io:format("Sent Message ...\n"),

    % close session link
    ok = amqp10_client:detach_link(Sender),

    % close off
    erlang:display("Closing Session ..."),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
    ok.

retrieve() ->
  io:format("Connecting ...\n"),

  {ok, Address} = application:get_env(myapp, "Address"),
  {ok, Hostname} = application:get_env(myapp, "Hostname"),
  {ok, Port} = application:get_env(myapp, "Port"),
  {ok, User} = application:get_env(myapp, "User"),
  {ok, Password} = application:get_env(myapp, "Password"),
  {ok, Container} = application:get_env(myapp, "Queue"),
  {ok, Queue} = application:get_env(myapp, "Queue"),
  OpnConf = #{
	      address => Address,
	      hostname => Hostname,
              port => Port,
	      notify => self(),
	      tls_opts => {secure_port, [{versions, ['tlsv1.1']}]},
              container_id => Container,
              sasl => {plain, User, Password}
	    },

  {ok, Connection} = amqp10_client:open_connection(OpnConf),

  receive
    {amqp10_event, {connection, Connection, opened}} ->
    io:format("Connected ...\n")
  after 2000 ->
    exit(connection_timeout)
  end,  

  {ok, Session} = amqp10_client:begin_session(Connection),
  receive
    {amqp10_event, {session, Session, begun}} ->
    	io:format("Session Id ...\n")
  after 2000 ->
    exit(session_timeout)
  end,

    erlang:display("Creating receiver ..."),
    % create a receiver link
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                                                    <<"test-receiver">>,
                                                    Queue),

    erlang:display("Getting credit ..."),

    % grant some credit to the remote sender but don't auto-renew it
    ok = amqp10_client:flow_link_credit(Receiver, 5, 3),

    erlang:display("Waiting Delivery ..."),

    % wait for a delivery
    receive
      {amqp10_msg, Receiver, InMsg} -> 
        erlang:display("Got Message ..."),
	erlang:display(InMsg),
        ok
    after 2000 ->
      exit(delivery_timeout)
    end,

    % close off
    erlang:display("Closing Session ..."),
    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection),
ok.

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
