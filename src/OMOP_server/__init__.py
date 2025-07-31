"""
OMOP_server: A modular, extensible ETL framework for transforming healthcare data 
into the OMOP Common Data Model (CDM) using Python, SQLAlchemy, and advanced concept mapping.
"""

__version__ = "0.1.0"
__author__ = "Siddharth Rajesh"
__email__ = "siddharth.rajesh03@gmail.com"

# Import main classes and functions for easy access
from .models import (
    ParentBase,
    Person,
    VisitOccurrence,
    ConditionOccurrence,
    ProcedureOccurrence,
    ObservationPeriod,
    ConceptBuilder,
    ConceptRelationship
)

from .utils import (
    connect_db,
    ConceptMapper,
    TransformerConceptMapper,
    ConfigManager
)

__all__ = [
    # Models
    "ParentBase",
    "Person", 
    "VisitOccurrence",
    "ConditionOccurrence",
    "ProcedureOccurrence", 
    "ObservationPeriod",
    "ConceptBuilder",
    "ConceptRelationship",
    
    # Utils
    "connect_db",
    "ConceptMapper",
    "TransformerConceptMapper", 
    "ConfigManager"
]