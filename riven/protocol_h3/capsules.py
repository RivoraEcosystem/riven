from dataclasses import dataclass
from typing import Literal

@dataclass(slots=True,frozen=True)
class WT_CLOSE_SESSION:
    """ sent when an application explicitly requests to shut down """

    type: Literal[0x2843] 
    "Capsule Type: 0x2843" # webtransport-draft16 
    
    length: int 
    "Total payload length" 
    
    Application_Error_Code: int = 0 
    "Error code by Application or 0 for no error" 
    
    Application_Error_Message: str = "" 
    "Error Message by Application or empty string for no error"