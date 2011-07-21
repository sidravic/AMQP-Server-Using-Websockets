require 'rubygems'
require 'amqp'
require 'mongo'
require 'em-websocket'
require 'json'

class MessageParser
  # message format                                        => "room:harry_potter, nickname:siddharth, room:members"
  def self.parse(message)
    parsed_message                                        = JSON.parse(message)
    puts "---PARSED MESSAGE ---" + parsed_message.inspect
    response                                              = {}
    if parsed_message['status'] == 'status'
      response[:status]                                   = 'STATUS'
      response[:username]                                 = parsed_message['username']
      response[:roomname]                                 = parsed_message['roomname']
    elsif parsed_message['status'] == 'message'
      response[:status]                                   = 'MESSAGE'
      response[:message]                                  = parsed_message['message']
      response[:username]                                 = parsed_message['username']
      response[:roomname]                                 = parsed_message['roomname'].split().join('_')
    end

    response
  end
end

class MongoManager
  def self.establish_connection(database)
    @db ||= Mongo::Connection.new('localhost', 27017).db(database)
    @rooms =  @db.collection('rooms')

    @db
  end

  def self.save_message(db, room_name, message, timestamp)
    posts ||=  db.collection("#{room_name.split().joins('_')}")


  end


end


@sockets = []
EventMachine.run do
  connection  = AMQP.connect(:host => '127.0.0.1')
  channel = AMQP::Channel.new(connection)
  puts "Connected to AMQP broker. #{AMQP::VERSION} "
  mongo = MongoManager.establish_connection("trackertalk_development")

  EventMachine::WebSocket.start(:host => '127.0.0.1', :port => 8080) do |ws|
    socket_detail = {:socket => ws}
    ws.onopen do
      @sockets << socket_detail
      puts " SOCKET INFO " + socket_detail.inspect
    end

    ws.onmessage do |message|
      puts "Message : #{message}"
      status = MessageParser.parse(message)
   #   puts "ROOMNAME : #{status[:roomname]} STATUS : #{status.inspect}"
      exchange = channel.fanout(status[:roomname].split().join('_'))
      exchange.publish("#{status[:username]} has just joined #{status[:roomname]}")
      posts = MongoManager.get_room_posts(mongo, status[:roomname])

      if status[:status] == 'STATUS'
        queue = channel.queue(status[:username])
        queue.unsubscribe  if queue.subscribed?
        queue.bind(exchange).subscribe do |payload|
          puts "PAYLOAD :  #{payload}"
          ws.send(payload)
        end

        # only after 0.8.0rc14
        #queue                                            = channel.queue(status[:username], :durable => true)
        #AMQP::Consumer.new(channel, queue)

      elsif status[:status] == 'MESSAGE'
        full_message = "<b> #{status[:username]} :</b>  #{status[:message]}"
        exchange.publish(full_message)
      end
    end

    ws.onclose do
      @sockets.delete ws
    end
  end

end
