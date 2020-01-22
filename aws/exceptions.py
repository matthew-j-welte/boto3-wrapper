"""This module contains all exception cases contained in the aws module"""

class ConstructorAccessDenied(Exception):
    """
    This is raised when a user tries to instantiate a class whos constructor
    has been modified to be private
    """
class InvalidUrlException(Exception):
    """
    This is raised when an invalid aws url is given to represent some resource
    """
class InvalidS3Type(Exception):
    """
    Raised when an invalid s3 type is given to a factory method
    """
