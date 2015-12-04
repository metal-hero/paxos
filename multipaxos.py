__author__ = 'roman'

import pika
import json
import hashlib
import threading
import time
import random
import pickle

DECREES = [['London','Paris','Madrid','Rome'],['Astana','Moscow','Stambul'],['Tokyo','Seul','Pekin']]

parameters = pika.ConnectionParameters(
        'localhost',
        5672,
        '/',
        pika.PlainCredentials('guest','guest'))


class Priest(object):
    m = 5 # Number of Priests
    def __init__(self, node_id):
        try:
            self = pickle.load( open( str(node_id)+"paxos_save.p", "rb" ) )
        except: 
            self.node_id = str(node_id) # self id
            self.master = None # id of Master
            self.last_vote = [] # List of votes
            self.last_ballot = None # id(time) of last ballot for this node
            self.curr_ballot = None # id(time) of current ballot
            self.E = 500 # epsilon - delay for action
            if int(self.node_id) == 1:
                self.master = self.node_id
            self.timekeys = [] # Times for checking Master
            self.ballots = [] # List of Quorums previous ballots (* when master role)
            self.votes = []  # List of Quorums previous votes (* when master role)
            self.quorum = []  # List of Quorum (* when master role)

            for i in range(Priest.m+1):
                self.timekeys.append(None)

            # setup channel
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue='find_master' + str(node_id))
            self.channel.queue_declare(queue='here_master' + str(node_id))
            self.channel.queue_declare(queue='next_ballot' + str(node_id))
            self.channel.queue_declare(queue='get_last_vote' + str(node_id))
            self.channel.queue_declare(queue='begin_ballot' + str(node_id))
            self.channel.queue_declare(queue='get_voted' + str(node_id))
            self.channel.basic_consume(self.here_master_callback,
                                  queue='here_master' + str(node_id),
                                  no_ack=True)
            self.channel.basic_consume(self.find_master_callback,
                                  queue='find_master' + str(node_id),
                                  no_ack=True)
            self.channel.basic_consume(self.next_ballot_callback,
                                  queue='next_ballot' + str(node_id),
                                  no_ack=True)
            self.channel.basic_consume(self.get_last_vote_callback,
                                  queue='get_last_vote' + str(node_id),
                                  no_ack=True)
            self.channel.basic_consume(self.begin_ballot_callback,
                                  queue='begin_ballot' + str(node_id),
                                  no_ack=True)
            self.channel.basic_consume(self.get_voted_callback,
                                  queue='get_voted' + str(node_id),
                                  no_ack=True)

        print "Searching"
        threading.Thread(target=self.run_find_master).start()
        threading.Thread(target=self.am_i_master).start()
        self.channel.start_consuming()


    def run_next_ballot(self,channel):
        self.last_ballot = self.curr_ballot
        self.curr_ballot = time.time()
        data = {'new_ballot': self.curr_ballot, 'master': self.master}
        self.votes = []
        self.ballots = []
        self.quorum = []
        for i in range(1,Priest.m+1): # Send to every Note by BroadCast
            channel.basic_publish(exchange='',
                                  routing_key='next_ballot'+str(i),
                                  body=json.dumps(data))


    def next_ballot_callback(self, channel, _, _2, body):
        data = json.loads(body)
        if int(data['new_ballot']) >= self.promise_ballot or self.promise_ballot is None: 
            self.master = data['master'] # Take promise !
            self.last_ballot = self.curr_ballot
            self.curr_ballot = data['new_ballot']
            self.promise_ballot = self.curr_ballot
            data = {'last_vote': self.last_vote, 'last_ballot': self.last_ballot, 'priest': self.node_id, 'curr_ballot': self.curr_ballot}
            channel.basic_publish(exchange='',
                                  routing_key='get_last_vote'+str(self.master),
                                  body=json.dumps(data))
        else:
            print 'I promised to '+self.curr_ballot


    def get_last_vote_callback(self, channel, _, _2, body):
        data = json.loads(body)
        if data['curr_ballot'] == self.curr_ballot and time.time() - self.curr_ballot > self.E:
            self.votes.append(data['last_vote'])
            self.ballots.append(data['last_ballot'])
            self.quorum.append(data['priest'])
            print str(self.quorum) + '<--- Quorum'
            print 'get_last_vote_callback'
        else:
            print 'Came different BALLOT!'

    # Thread checker
    def am_i_master(self):
        while True:
            print 'I am ' + self.node_id
            print 'Master => ' + str(self.master)
            num_of_dead = 0
            for i in range(1,int(self.node_id)):
                if self.timekeys[i] is None or time.time() - self.timekeys[i] > self.E:
                    num_of_dead += 1
            if num_of_dead >= int(self.node_id)-1:
                if self.master == self.node_id:
                    print 'Node '+ self.node_id + ' is Master !'
                else:
                    print 'Node' + self.node_id + ' is NEW Master !'
                    self.master = self.node_id
                    self.run_next_ballot()
            else:
                self.master = None
            time.sleep(10)

    # Main Thread implementation
    def run_find_master(self):
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        while True:
            if len(self.quorum)> Priest.m/2:
                self.begin_ballot()
            # print self.master + '<--- MASTER'
            #if int(self.node_id) <= Priest.m/2 and int(self.node_id) != 1:
            for i in range(1,int(self.node_id)):
                try:
                    channel.basic_publish(exchange='',
                                          routing_key='find_master'+str(i),
                                          body=self.node_id)
                    #print str(i) + ' run find master'
                except:
                    continue
            pickle.dump( self, open( str(node_id)+"save.p", "wb" )) 
            time.sleep(1)

    def find_master_callback(self, channel, _, _2, body):
        channel.basic_publish(exchange='',
                              routing_key='here_master'+body,
                              body=self.node_id)
        print "find master callback"

    def here_master_callback(self, channel, _, _2, body):
        print "here master callback"
        self.timekeys[int(body)] = time.time()
        print self.timekeys


    def begin_ballot(self):
        if len(self.quorum)> Priest.m/2:
            max_ballot = None
            super_last_vote = []
            for i in range(len(self.ballots)):
                if max_ballot is None or self.ballots[i] > max_ballot:
                    max_ballot = self.ballots[i]
                    super_last_vote = self.votes[i] 
            if max_ballot is None or super_last_vote is None:
                for i in range(len(DECREES)):
                    super_last_vote.append(DECREES[i][random.randint(0, len(DECREES[i])-1)])
            print 'SUPER LAST VOTE => '
            print super_last_vote
            for i in self.quorum:
                self.channel.basic_publish(exchange='',
                                      routing_key='begin_ballot'+str(i),
                                      body=json.dumps(super_last_vote))
        else: 
            'NOT ENOUGH! NO QUORUM!'
        print "begin ballot"

    def begin_ballot_callback(self, channel, _, _2, body):
        self.last_vote = json.loads(body)
        print json.loads(body)
        print self.node_id + ' / last vote ' + self.last_vote 
        channel.basic_publish(exchange='',
                              routing_key='get_voted'+str(self.master),
                              body=self.node_id)
        print "begin ballot callback"

    # def get_voted(self, channel, _, _2, body):
    #     print "get voted"

    def get_voted_callback(self, channel, _, _2, body):
        print body+" get voted callback !!!"










