import os
import logging
import slack_sdk
import slack_sdk.errors as slack_errors

logger = logging.getLogger(__name__)

SLACK_CLIENT: "SlackClientWrapper" = None


class SlackClientWrapper():
    def __init__(self) -> None:
        self.slack_channel = os.getenv('HDX_SLACK_NOTIFICATION_CHANNEL')
        # self.slack_channel = 'test-channel'


        self.slack_client = None
        token = os.getenv('HDX_SLACK_NOTIFICATION_ACCESS_TOKEN')
        if token:
            self.slack_client = slack_sdk.WebClient(token=token)
            logger.debug('Slack client initialized')

    def post_to_slack_channel(self, message: str):
        if self.slack_client:
            try:
                text = f'[PCode BOT] {message}'
                response = self.slack_client.chat_postMessage(channel=self.slack_channel, text=text)
            except slack_errors.SlackApiError as e:
                # You will get a SlackApiError if "ok" is False
                # assert e.response["ok"] is False
                logger.error(f"Got an error: {e.response['error']}")
        else:
            logger.info(f'[instead of slack] {message}')


def get_slack_client():
    global SLACK_CLIENT
    if not SLACK_CLIENT:
        SLACK_CLIENT = SlackClientWrapper()
    return SLACK_CLIENT