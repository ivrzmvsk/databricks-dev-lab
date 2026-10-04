import json
import os
import time

from dotenv import load_dotenv
load_dotenv()

from azure.eventhub import EventHubProducerClient, EventData
from requests_sse import EventSource

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


def fetch_and_send():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=os.getenv("EVENTHUB_CONNECTION_STRING"),
    )

    print("Connected to Event Hub")

    headers = {"User-Agent": os.getenv("USER_AGENT", "ivanrazumovskyi")}

    with EventSource(STREAM_URL, headers=headers) as stream:
        for event in stream:
            try:
                if event.type != "message":
                    continue

                change = json.loads(event.data)

                # Wikimedia sends synthetic "canary" events on this stream as a
                # health check / heartbeat -- they aren't real edits, so skip them
                if change.get("meta", {}).get("domain") == "canary":
                    continue

                # Only keep actual page edits -- the stream also carries page
                # creations, log actions (blocks, deletions, etc.), and
                # category changes, which we don't want here
                if change.get("type") != "edit":
                    continue

                producer.send_event(EventData(json.dumps(change)))
                print(f"Sent edit by {change.get('user')} on {change.get('title')}")

            except json.JSONDecodeError:
                print("Skipping malformed event")
                continue
            except Exception as e:
                print(f"An error occurred: {e}")
                time.sleep(5)


if __name__ == "__main__":
    try:
        fetch_and_send()
    except KeyboardInterrupt:
        print("Keyboard interrupt, exiting...")
