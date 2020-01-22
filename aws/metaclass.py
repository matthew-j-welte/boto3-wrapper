from botocore.exceptions import NoCredentialsError, CredentialRetrievalError, ClientError
from functools import wraps
from logging import Logger
from threading import Thread
from types import FunctionType
from time import sleep

EXPIRED_TOKEN = "ExpiredToken"
MAX_RETRIES = 10

_logger = Logger(__name__)

def decide_on_async(func):
    @wraps(func)
    def async_func(*args, **kwargs):
        if kwargs.get("run_async"):
            background_function = Thread(target=func, args=args, kwargs=kwargs)
            background_function.start()
            return background_function
        return func(*args, **kwargs)
    return async_func

def handle_credential_errors(func):
    @wraps(func)
    def handled_function(*args, **kwargs):
        retries = 0
        while retries < MAX_RETRIES:
            try:
                return func(*args, **kwargs)
            except (NoCredentialsError, CredentialRetrievalError):
                retries, result = __handle_retry(retries, func, *args, **kwargs)
                if result:
                    return result
            except ClientError as e:
                if e.response["Error"]["Code"] == EXPIRED_TOKEN:
                    retries, result = __handle_retry(retries, func, *args, **kwargs)
                    if result:
                        return result
    return handled_function

def __handle_retry(retries, func, *args, **kwargs):
    result = None
    try:
        _logger.warning(f"<function {func.__name__}> raised credential error - retrying...")
        _logger.warning(f"Attempts Remaining: {MAX_RETRIES - retries}")
        retries += 1
        sleep(1)
        result = func(*args, **kwargs)
    finally:
        return retries, result if result else None

class AWSMetaClass(type):
    def __new__(meta, classname, bases, classDict):
        newClassDict = {}
        for attributeName, attribute in classDict.items():
            if isinstance(attribute, FunctionType):
                attribute = decide_on_async(handle_credential_errors(attribute))
            newClassDict[attributeName] = attribute
        return type.__new__(meta, classname, bases, newClassDict)
