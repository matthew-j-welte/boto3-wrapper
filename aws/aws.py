from aws.metaclass import AWSMetaClass

"""
This module contains the parent AWS class which can be inherited from
by any other aws class being created. It has properties that can be used
to create clients and resources on the go as long as the child class
overrides the name property
"""

from abc import abstractproperty
import boto3

class AWS(metaclass=AWSMetaClass):
    """Parent class of all aws class wrappers"""
    @abstractproperty
    def name(self):
        """The name that you would pass into a boto3.client(<name>) call (for example)"""
        raise NotImplementedError("Must define a name property")
    @property
    def session(self):
        """Returns a newly refreshed boto3 session"""
        return boto3.session.Session()
    @property
    def client(self):
        """Returns a boto3 client with a newly refreshed boto3 session"""
        return self.session.client(self.name)
    @property
    def resource(self):
        """Returns a boto3 resource with a newly refreshed boto3 session"""
        return self.session.resource(self.name)
