import json
import logging
import os
import time
import uuid
from typing import Any, Dict

import boto3
from dotenv import load_dotenv


class SQSEventHandler:
    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_region_name: str,
        input_queue_name: str,
        output_queue_name: str,
    ):
        """
        Initialize SQS Event Handler with input and output queues

        :param aws_access_key_id: AWS access key ID
        :param aws_secret_access_key: AWS secret access key
        :param aws_region_name: AWS region where queues are located
        :param input_queue_name: Name of the input SQS queue
        :param output_queue_name: Name of the output SQS queue
        """

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        self.logger = logging.getLogger(__name__)

        # Create SQS client
        self.sqs = boto3.client(
            "sqs",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region_name,
        )

        # Get queue URLs
        self.input_queue_url = self._get_queue_url(input_queue_name)
        self.output_queue_url = self._get_queue_url(output_queue_name)

        self.logger.info(f"Initialized SQS Event Handler")
        self.logger.info(f"Input Queue: {self.input_queue_url}")
        self.logger.info(f"Output Queue: {self.output_queue_url}")

    def _get_queue_url(self, queue_name: str) -> str:
        """
        Retrieve the URL for a given queue name

        :param queue_name: Name of the SQS queue
        :return: Queue URL
        """
        try:
            response = self.sqs.get_queue_url(QueueName=queue_name)
            return response["QueueUrl"]
        except self.sqs.exceptions.QueueDoesNotExist:
            self.logger.error(f"Queue {queue_name} does not exist")
            raise

    def receive_messages(self, max_messages: int = 10, wait_time: int = 20) -> list:
        """
        Receive messages from the input queue

        :param max_messages: Maximum number of messages to receive
        :param wait_time: Long polling wait time in seconds
        :return: List of received messages
        """
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.input_queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
                MessageAttributeNames=["All"],
            )
            return response.get("Messages", [])
        except Exception as e:
            self.logger.error(f"Error receiving messages: {e}")
            return []

    def process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process an individual message. Override this method for custom processing.

        :param message: SQS message dictionary
        :return: Processed event dictionary
        """
        try:
            # Parse the message body
            body = json.loads(message["Body"])

            # Example processing: add a processing timestamp
            body["processed_at"] = time.time()
            body["event_id"] = str(uuid.uuid4())

            return body
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            return None

    def send_event(self, event: Dict[str, Any]):
        """
        Send an event to the output queue

        :param event: Event dictionary to send
        """
        try:
            response = self.sqs.send_message(
                QueueUrl=self.output_queue_url, MessageBody=json.dumps(event)
            )
            self.logger.info(f"Sent event to output queue: {response['MessageId']}")
        except Exception as e:
            self.logger.error(f"Error sending event: {e}")

    def delete_message(self, message: Dict[str, Any]):
        """
        Delete a processed message from the input queue

        :param message: Message to delete
        """
        try:
            self.sqs.delete_message(
                QueueUrl=self.input_queue_url, ReceiptHandle=message["ReceiptHandle"]
            )
            self.logger.info("Message deleted from input queue")
        except Exception as e:
            self.logger.error(f"Error deleting message: {e}")

    def run(self):
        """
        Main event loop to continuously listen and process messages
        """
        self.logger.info("Starting SQS Event Handler")
        try:
            while True:
                # Receive messages
                messages = self.receive_messages()

                for message in messages:
                    try:
                        # Process the message
                        processed_event = self.process_message(message)

                        if processed_event:
                            # Send processed event to output queue
                            self.send_event(processed_event)

                            # Delete the original message
                            self.delete_message(message)

                    except Exception as e:
                        self.logger.error(f"Error in message processing loop: {e}")

        except KeyboardInterrupt:
            self.logger.info("SQS Event Handler stopped by user")
        except Exception as e:
            self.logger.error(f"Unexpected error in SQS Event Handler: {e}")


def main():
    load_dotenv()

    handler = SQSEventHandler(
        aws_access_key_id=os.getenv("AWS_SQS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SQS_SECRET_ACCESS_KEY"),
        aws_region_name=os.getenv("AWS_SQS_REGION_NAME"),
        input_queue_name=os.getenv("AWS_SQS_INPUT_QUEUE_NAME"),
        output_queue_name=os.getenv("AWS_SQS_OUTPUT_QUEUE_NAME"),
    )
    handler.run()


if __name__ == "__main__":
    main()
