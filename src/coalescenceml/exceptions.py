class CoMLException(Exception):
    """
    Generic class for CoalescenceML exceptions.
    """

    def __init__(self, message):
        """
        Args:
          message: A message detailing the error that occurred.
        """
        self.message = message
        super().__init__(message)
