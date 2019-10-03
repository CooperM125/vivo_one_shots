from abc import ABC, abstractmethod

from utils import Aide


class AuditABC(ABC):

    def __init__(self, aide: Aide):
        self.aide = Aide
        self.repeat = False
        self.subject_types = []

    @abstractmethod
    def get_sub_trips(self, subject: str) -> list:
        pass

    @abstractmethod
    def get_add_trips(self, subject: str) -> list:
        pass

    def applies_to_type(self, sub_type: str) -> bool:
        return sub_type in self.subject_types
