class BaseTransformer(ABC):
    """Base class for all data transformers."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self._setup_storage()
    
    def _setup_storage(self) -> None:
        """Set up silver and gold data storage."""
        self.silver_path = Path(self.config['data_paths']['silver'])
        self.gold_path = Path(self.config['data_paths']['gold'])
        self.silver_path.mkdir(parents=True, exist_ok=True)
        self.gold_path.mkdir(parents=True, exist_ok=True)
    
    @abstractmethod
    def transform(self, input_path: Path, **kwargs) -> Path:
        """Transform data from one layer to another."""
        pass