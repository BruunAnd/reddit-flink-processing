import praw
import json
import kafka
import os

reddit = praw.Reddit(client_id=os.environ.get('REDDIT_CLIENT'),
                     client_secret=os.environ.get('REDDIT_SECRET'),
                     user_agent='Kafka Publisher')

producer = kafka.KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))

if __name__ == '__main__':
    for comment in reddit.subreddit("all").stream.comments():
        producer.send('reddit-comments', dict(message=comment.body,
                                              subreddit=comment.subreddit.display_name.lower(),
                                              time=int(comment.created_utc),
                                              author=comment.author.name))

    