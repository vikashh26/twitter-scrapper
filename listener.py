import pika
from json import loads
from os import getenv
import snscrape.modules.twitter as sntwitter
from pymongo import MongoClient
from dotenv import load_dotenv


load_dotenv('./.env')
RABBIT_MQ = getenv("RABBIT_MQ")

MONGO_URL = getenv("MONGO_URL")
MONGO_DB_NAME = getenv("MONGO_DB_NAME")

QUEUE = getenv("TREND_QUEUE")

TWEETS = 100

mongo_db = MongoClient(MONGO_URL)[MONGO_DB_NAME]

class RabbitMqListener:
    def start_consuming(self):
        # establish connection to RabbitMQ
        print(" [x] established connection to RabbitMQ")
        connection = pika.BlockingConnection(
            pika.URLParameters(RABBIT_MQ))
        self.channel = connection.channel()

        # declare a queue
        self.channel.queue_declare(queue=QUEUE, durable=True)

        # set the number of tasks each worker can handle at once
        self.channel.basic_qos(prefetch_count=1)

        # consume from the queue
        self.channel.basic_consume(queue=QUEUE, on_message_callback=self.callback)

        print(' [i] Waiting for tasks. To exit press CTRL+C')
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def process_task(self):
        since_id = self.job.get('since_id')
        trend_name = self.job.get('trend_name')
        if not trend_name:
            return None
        trend_id = self.job.get('_id')
        campaign_id = self.job.get('campaign_id')
        print(f' [i] processing {trend_name}...')
        for i,tweet in enumerate(sntwitter.TwitterSearchScraper(trend_name).get_items()):
            tweet_id = str(tweet.id)
            if i == TWEETS or since_id == tweet_id:
                break

            tweet.insert_tweet(mongo_db,trend_id,trend_name,campaign_id)

        print("Done !!!")
        return True

    def callback(self,ch, method, properties, body):
        print(' [i] Job received...')
        message = loads(body.decode())
        self.job = message
        self.process_task()
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(' [i] Job acknowledged')
        return True

    def listen(self):
        while True:
            try:
                self.start_consuming()
            except pika.exceptions.ConnectionClosedByBroker:
                # handle the connection close event
                print(" [e] Connection closed by broker, trying to reconnect...")
                continue
            except KeyboardInterrupt:
                # handle keyboard interrupt
                self.stop_consuming()
                print(" [o] Interrupted by user, exiting...")
                break

            
if __name__ == '__main__':
    worker = RabbitMqListener()
    worker.listen()