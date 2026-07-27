from typing import Any

class RivenException(Exception):
    """Base Exception for all Riven Exceptions"""

class InvalidEvent(RivenException):
    """Event handler received invalid or unsupported event type"""
    def __init__(self,event:Any):
        self.event = event

        if isinstance(event, str):
            # Event is already a text
            event_name =    event
        elif hasattr(event, "__name__"):
            # Event is a class object
            event_name = event.__name__
        elif hasattr(event, "__class__"):
            # Event is an instance or a basic data type (int, dict, etc)
            # This will result in int, dict or the custom class name
            event_name = event.__class__.__name__
        else:
            # Absolute fallback
            event_name = str(event)

        super().__init__(f"Unsupported event type: {event_name}")