class BaseLoader(ABC):
    """Base class for all data loaders."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def load(self, input_path: Path, **kwargs) -> None:
        """Load data into target storage."""
        pass