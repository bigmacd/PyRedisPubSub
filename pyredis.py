import redis
import threading
import argparse

# This example uses a thread, so to tell the thread to stop we
# publish this message
END_STRING = "finish"

# This class runs as a thread and displays message published to the channel
# to which it is subscribed
class RedisClient(threading.Thread):
    """ Creates the Redis Client and subscribes to the initial channel """
    def __init__(self, channels):
        threading.Thread.__init__(self)
        self.redis = redis.Redis(host="localhost", port=6379)
        self.pubsub = self.redis.pubsub()
        self.channels = channels
        self.pubsub.subscribe(self.channels)


    def subscribe(self, channels):
        """ The main thread prompts to subscribe to a new channels
            so this function allow the main thread to change the subscription
        """
        self.pubsub.unsubscribe(self.channels)
        self.channels = channels
        self.pubsub.subscribe(channels)


    def outputMessage(self, channel, data):
        """ Display to the terminal the message received on the subscribed channel """
        message = "{0}:{1}".format(channel, data)
        print(message)


    def run(self):
        """ The thread 'main'.  Listen for message published by whomever.
            Ignore subscribe and unsubscribe nessages.  Look for the
            published message to say exit (END_STRING)
        """
        # item['type'] contains a string
        # item['channel'] contains bytes
        # item['data'] contains bytes
        for item in self.pubsub.listen():
            if item['type'] in ("subscribe", "unsubscribe"):
                continue
            if str(item['data'], 'utf-8') in (END_STRING,):
                self.pubsub.unsubscribe()
                break
            self.outputMessage(str(item['channel'], 'utf-8'), str(item['data'], 'utf-8'))


if __name__ == "__main__":

    # Accept a command line parameter as the channel to which to subscribe
    parser = argparse.ArgumentParser()
    parser.add_argument("--sub", help="channel to subscribe...to")
    args = parser.parse_args()
    
    # if no command line parameter was specified, default to 'test'
    subscription = args.sub if args.sub is not None else "test"
    
    # be a client so we can publish messages, specifically END_STRING
    r = redis.Redis(host="localhost", port=6379)
    client = RedisClient([subscription])
    client.start()

    # remember to which channel was last subscribed, so we can publish our
    # END_STRING message
    lastSubscription = subscription
    while True:
        subscription = input("change subscription ('{0}' to quit): ".format(END_STRING))
        # got something from the terminal, it is supposed to be a channel 
        # or the keyword equal to END_STRING ('finish')
        if subscription != END_STRING:
            lastSubscription = subscription
            print("changing subscription to {0}".format(subscription))
        else:
            print("exiting now...")
            r.publish(lastSubscription, END_STRING)
            break

        client.subscribe(subscription)
