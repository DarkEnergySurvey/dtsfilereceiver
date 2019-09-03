""" Utility classes and functions for decade file processing
"""
from dtsfilereceiver.decadeFileBase import DecadeFileBase
import json
import sys
import logging
import time
import datetime
from logging.handlers import TimedRotatingFileHandler

from globus_sdk import (NativeAppAuthClient, TransferClient, AuthClient,
                        RefreshTokenAuthorizer, TransferData)
from globus_sdk.exc import GlobusAPIError


class GlobusConnection(DecadeFileBase):
    REDIRECT_URI = 'https://auth.globus.org/v2/web/auth-code'
    SCOPES = ('openid email profile '
              'urn:globus:auth:scope:transfer.api.globus.org:all')
    TRANSLIMIT = 25

    def __init__(self, config, tag):
        DecadeFileBase.__init__(self, config, tag)
        self.client = None
        self.active_transfer_count = 0
        self.noao_dirs = []
        self.scantime = 0.
        self.starttime = datetime.datetime.now()
        self.active_transfer_count = 0
        self.number_started = 0
        self.number_successful = 0
        self.number_failed = 0
        self.number_waiting = 0

    def __del__(self):
        self.close()
        logging.shutdown()

    def load_tokens_from_file(self):
        """Load a set of saved tokens."""
        with open(self.config['token_file'], 'r') as _file:
            tokens = json.load(_file)
        return tokens

    def save_tokens_to_file(self, tokens):
        """Save a set of tokens for later use."""
        with open(self.config['token_file'], 'w') as _file:
            json.dump(tokens, _file)

    def update_tokens_file_on_refresh(self, token_response):
        """
        Callback function passed into the RefreshTokenAuthorizer.
        Will be invoked any time a new access token is fetched.
        """
        self.save_tokens_to_file(token_response.by_resource_server)

    def initiate_connection(self):
        """ Initiate the connection
        """
        tokens = None
        try:
            # if we already have tokens, load and use them
            tokens = self.load_tokens_from_file()
        except:
            pass

        if not tokens:
            # if we need to get tokens, start the Native App authentication process
            client = NativeAppAuthClient(client_id=self.CLIENT_ID)#self.config['client_id'])
            # pass refresh_tokens=True to request refresh tokens
            client.oauth2_start_flow(requested_scopes=self.SCOPES,#self.config['requested_scopes'],
                                     redirect_uri=self.REDIRECT_URI,#self.config['redirect_uri'],
                                     refresh_tokens=True)

            url = client.oauth2_get_authorize_url()

            print 'Native App Authorization URL: \n{}'.format(url)

            auth_code = raw_input('Enter the auth code: ').strip()

            token_response = client.oauth2_exchange_code_for_tokens(auth_code)

            # return a set of tokens, organized by resource server name
            tokens = token_response.by_resource_server

            try:
                self.save_tokens_to_file(tokens)
            except:
                pass

        transfer_tokens = tokens['transfer.api.globus.org']

        auth_client = NativeAppAuthClient(client_id=self.config['client_id'])

        authorizer = RefreshTokenAuthorizer(
            transfer_tokens['refresh_token'],
            auth_client,
            #access_token=transfer_tokens['access_token'],
            #expires_at=transfer_tokens['expires_at_seconds'],
            on_refresh=self.update_tokens_file_on_refresh)

        self.client = TransferClient(authorizer=authorizer)

        # print out a directory listing from an endpoint
        try:
            #print 'ACTIVATE'
            #print 'DEST',self.config['dest_ep']
            self.client.endpoint_autoactivate(self.config['dest_ep'])
            ac = self.client.endpoint_get_activation_requirements(self.config['dest_ep'])
            #print ac
            self.client.endpoint_autoactivate(self.config['src_ep'])
            ac2 = self.client.endpoint_get_activation_requirements(self.config['src_ep'])
            #print ac2
        except GlobusAPIError as ex:
            self.logger.error('Error in endpoint activation %s', str(ex))

            if ex.http_status == 401:
                sys.exit('Refresh token has expired. '
                         'Please delete refresh-tokens.json and try again.')
            else:
                raise ex

    def get_dirs(self):
        """ function
        """
        now = time.time()
        full_dirs = self.client.operation_ls(self.config['src_ep'], path=self.config['noao_root'])
        self.scantime = time.time() - now
        for entry in full_dirs:
            if entry['type'].lower() == 'dir':
                self.noao_dirs.append(str(entry['name']))



