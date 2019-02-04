from abc import ABC, abstractmethod


class AbstractPeer(ABC):
    @abstractmethod
    def send(self, data):
        """Send bytes.
        
        Args:
            data (bytes): the bytes to send.
        """
        pass

    @abstractmethod
    def receive(self, receive_size):
        """Read bytes.

        Args:
            receive_size (number): the size to receive/read.
        
        Returns:
            bytes. the read bytes.
        """
        pass
