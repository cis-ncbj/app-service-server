#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Wrapper for RabbitMQ.
"""

import os
import logging
import pika
import time

import Globals as G
from Config import conf, VERBOSE, ExitCodes


logger = logging.getLogger(__name__)

class Rabbit(object):
    """
    This class wraps the pika interface to RabbitMQ. Adds some AppServer
    specific utilities.
    """

    def __init__(self):
        self.__credentials = None  #: RabbitMQ credentaials e.g. pika.PlainCredentials
        self.__connection = None  #: Connection object
        self.__channel = None  #: Channel object

    def init(self, queues):
        """
        Inintialize RabbitMQ connection and define queues.

        :param queues: list with queue definitions. Each entry should be a
            dictionary containing 'name' of the queue and boolean parameter
            'durable' specifying if queue should be persisted.
        """
        self.__credentials = pika.PlainCredentials(conf.config_rabbit_user,
                                                   conf.config_rabbit_password)
        self.__connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=conf.config_rabbit_host,
                    port=conf.config_rabbit_port,
                    virtual_host=conf.config_rabbit_vhost,
                    credentials=self.__credentials
                    ))
        self.__channel = self.__connection.channel()
        # Default channel should be a secure one. If a faster communication
        # would be requierad aither batching or second fast channel could be
        # added.
        self.__channel.confirm_delivery()

        # Declare all queueus
        for _q in queues:
            # Declare the queue
            self.__channel.queue_declare(queue=_q['name'],
                                         durable=_q['durable'])
            # For durable queues create delayed queue that can be used for
            # requeueing messages
            if _q['durable']:
                self.__channel.queue_declare(
                        queue="%s_delayed"%_q['name'],
                        durable=_q['durable'],
                        arguments={
                            # Delay until the message is transferred in
                            # milliseconds.
                            'x-message-ttl' : conf.config_rabbit_requeue_delay,
                            # Exchange used to transfer the message from
                            # delayed to the main queue.
                            'x-dead-letter-exchange' : 'amq.direct',
                            # Name of the queue where messeges will be
                            # delivered when ttl is reached.
                            'x-dead-letter-routing-key' : _q['name']
                            }
                        )

    def send(self, queue, message, persist=True):
        _done = False
        _sleep = 1

        while not _done:
            try:
                self.__send_imp(queue, message, persist)
                _done = True
            except pika.exceptions.ConnectionClosed as exc:
                logger.warning('Connection to RabbitMQ closed. Will retry.')
            time.sleep(_sleep)
            _sleep *= 2

    def resend(self, queue, message, persist=True, delayed=True):
        _done = False
        _sleep = 1
        _name = queue
        if delayed:
            _name = "%s_delayed" % queue

        while not _done:
            try:
                self.__send_imp(_name, message, persist)
                _done = True
            except pika.exceptions.ConnectionClosed as exc:
                logger.warning('Connection to RabbitMQ closed. Will retry.')
            time.sleep(_sleep)
            _sleep *= 2

    def __send_imp(self, queue, message, persist=True):
        _mode = 1
        if persist:
            _mode = 2

        self.__channel.basic_publish(
                exchange='',
                routing_key=queue,
                body=message,
                properties=pika.BasicProperties(delivery_mode=_mode))
