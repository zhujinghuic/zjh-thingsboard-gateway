from abc import ABC, abstractmethod


class PsPackHandler(ABC):

    @abstractmethod
    def createProcess(self):
        pass

    @abstractmethod
    def process(self):
        pass
