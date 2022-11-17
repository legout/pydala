import functools
import logging
import os
import sys
import time
from datetime import timedelta
from inspect import getframeinfo, stack


class CustomFormatter(logging.Formatter):
    """Custom Formatter does these 2 things:
    1. Overrides 'funcName' with the value of 'func_name_override', if it exists.
    2. Overrides 'filename' with the value of 'file_name_override', if it exists.
    """

    def format(self, record):
        if hasattr(record, "func_name_override"):
            record.funcName = record.func_name_override
        if hasattr(record, "file_name_override"):
            record.filename = record.file_name_override
        return super(CustomFormatter, self).format(record)


def get_logger(name: str, log_file: str | None = None, log_sub_dir: str | None = None):
    """Creates a Log File and returns Logger object"""

    logger = logging.Logger(name)
    logger.setLevel(logging.INFO)
    """ Set the formatter of 'CustomFormatter' type as we need to log base 
    function name and base file name """
    formatter = CustomFormatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s"
    )
    # formatter = logging.Formatter(
    #    "%(asctime)s | %(name)s | %(levelname)s | %(funcName)s | %(message)s"
    # )
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    if log_file:
        log_sub_dir = log_sub_dir or ""
        log_dir = "logs"
        log_dir = os.path.join(log_dir, log_sub_dir)

        # Create Log file directory if not exists
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Build Log File Full Path
        logPath = (
            log_file
            if os.path.exists(log_file)
            else os.path.join(log_dir, (str(log_file) + ".log"))
        )

        # Create logger object and set the format for logging and other attributes
        handler = logging.FileHandler(logPath, "a+")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Return logger object
    return logger


def log_decorator(_func: str | None = None, show_arguments:bool=True):
    def log_decorator_info(func):
        @functools.wraps(func)
        def log_decorator_wrapper(self, *args, **kwargs):
            # Build logger object
            logger = get_logger(
                name=repr(self.__class__).split("'")[1],
                log_file=self._log_file,
                log_sub_dir=self._log_sub_dir,
            )
            # if not logger:
            # logger = self.logger

            """ Create a list of the positional arguments passed to function.
            - Using repr() for string representation for each argument. repr() is similar to str() only difference being
             it prints with a pair of quotes and if we calculate a value we get more precise value than str(). """
            args_passed_in_function = [repr(a) for a in args]
            """ Create a list of the keyword arguments. The f-string formats each argument as key=value, where the !r 
                specifier means that repr() is used to represent the value. """
            kwargs_passed_in_function = [f"{k}={v!r}" for k, v in kwargs.items()]

            """ The lists of positional and keyword arguments is joined together to form final string """
            formatted_arguments = ", ".join(
                args_passed_in_function + kwargs_passed_in_function
            )

            """ Generate file name and function name for calling function. __func.name__ will give the name of the 
                caller function ie. wrapper_log_info and caller file name ie log-decorator.py
            - In order to get actual function and file name we will use 'extra' parameter.
            - To get the file name we are using in-built module inspect.getframeinfo which returns calling file name """
            # py_file_caller = getframeinfo(stack()[1][0])
            extra_args = {
                "func_name_override": func.__name__,
                # "file_name_override": os.path.basename(py_file_caller.filename),
            }

            """ Before to the function execution, log function details."""
            if show_arguments:
                logger.info(f"Arguments: {formatted_arguments}. Start.", extra=extra_args)
            else:
                logger.info(f"Start.", extra=extra_args)
            start = time.time()
            try:
                """log return value from the function"""
                value = func(self, *args, **kwargs)
                # logger.info(f"Returned: - End function {value!r}")
                logger.info(
                    f"Finished. Elapsed time: {time.strftime('%Hh %Mm %Ss', time.gmtime(time.time()-start)) }.",
                    extra=extra_args,
                )
            except:
                """log exception if occurs in function"""
                logger.error(f"Exception: {str(sys.exc_info()[1])}", extra=extra_args)
                raise
            # Return function value
            return value

        # Return the pointer to the function
        return log_decorator_wrapper

    # Decorator was called with arguments, so return a decorator function that can read and return a function
    if _func is None:
        return log_decorator_info
    # Decorator was called without arguments, so apply the decorator to the function immediately
    else:
        return log_decorator_info(_func)
