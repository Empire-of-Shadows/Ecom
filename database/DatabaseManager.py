import os
import asyncio
import signal
import json
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
from pathlib import Path
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ServerSelectionTimeoutError, ConfigurationError, OperationFailure
from dotenv import load_dotenv

from loggers.logger_setup import get_logger, PerformanceLogger, log_performance, log_context

# Load environment variables
load_dotenv()
ECOM_DATABASE = os.getenv("ECOM_DATABASE")

logger = get_logger("DatabaseManager", level=20, json_format=False, colored_console=True)


class DatabaseConnectionError(Exception):
    """Custom exception for database connection issues"""
    pass


class DatabaseOperationError(Exception):
    """Custom exception for database operation issues"""
    pass


# Global database mapping storage
DATABASE_MAPPINGS: Dict[str, Any] = {}
COLLECTION_REGISTRY: Dict[str, Dict[str, Any]] = {}


class DatabaseManager:
    """
    Enhanced shared database manager with auto-discovery and global mappings.
    Supports multiple databases with dynamic collection mapping.
    """

    def __init__(
            self,
            connection_timeout: int = 10000,
            server_selection_timeout: int = 5000,
            max_pool_size: int = 50,
            min_pool_size: int = 10,
            max_idle_time: int = 30000,
            retry_writes: bool = True,
            retry_reads: bool = True,
            heartbeat_frequency: int = 10000,
            health_check_interval: int = 30,
            auto_discover: bool = True
    ):
        """
        Initialize DatabaseManager with enhanced configuration and auto-discovery.

        Args:
            connection_timeout: Connection timeout in milliseconds
            server_selection_timeout: Server selection timeout in milliseconds
            max_pool_size: Maximum connection pool size
            min_pool_size: Minimum connection pool size
            max_idle_time: Maximum idle time for connections in milliseconds
            retry_writes: Enable automatic retry for write operations
            retry_reads: Enable automatic retry for read operations
            heartbeat_frequency: Heartbeat frequency in milliseconds
            health_check_interval: Health check interval in seconds
            auto_discover: Enable automatic database and collection discovery
        """
        # Connection settings
        self.connection_timeout = connection_timeout
        self.server_selection_timeout = server_selection_timeout
        self.max_pool_size = max_pool_size
        self.min_pool_size = min_pool_size
        self.max_idle_time = max_idle_time
        self.retry_writes = retry_writes
        self.retry_reads = retry_reads
        self.heartbeat_frequency = heartbeat_frequency
        self.health_check_interval = health_check_interval
        self.auto_discover = auto_discover

        # Connection state
        self.db_client: Optional[AsyncIOMotorClient] = None
        self._initialized = False
        self._connection_healthy = False
        self._health_check_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        # Database registry
        self.databases: Dict[str, Any] = {}
        self.collections: Dict[str, Any] = {}

        # Metrics tracking
        self.metrics = {
            "connection_attempts": 0,
            "successful_connections": 0,
            "failed_connections": 0,
            "reconnection_attempts": 0,
            "health_check_failures": 0,
            "last_connection_time": None,
            "last_health_check": None,
            "total_operations": 0,
            "failed_operations": 0,
            "databases_discovered": 0,
            "collections_discovered": 0
        }

        logger.info("DatabaseManager initialized with enhanced configuration and auto-discovery")
        self._log_configuration()

    def _log_configuration(self):
        """Log current configuration settings"""
        config_info = {
            "connection_timeout": f"{self.connection_timeout}ms",
            "server_selection_timeout": f"{self.server_selection_timeout}ms",
            "max_pool_size": self.max_pool_size,
            "min_pool_size": self.min_pool_size,
            "max_idle_time": f"{self.max_idle_time}ms",
            "retry_writes": self.retry_writes,
            "retry_reads": self.retry_reads,
            "heartbeat_frequency": f"{self.heartbeat_frequency}ms",
            "health_check_interval": f"{self.health_check_interval}s",
            "auto_discover": self.auto_discover
        }

        logger.info(f"Database configuration: {config_info}")

    @log_performance("database_initialization")
    async def initialize(self, max_retries: int = 3, retry_delay: float = 2.0) -> bool:
        """
        Initialize database connections with auto-discovery and comprehensive error handling.

        Args:
            max_retries: Maximum number of connection retry attempts
            retry_delay: Delay between retry attempts in seconds

        Returns:
            bool: True if initialization successful, False otherwise
        """
        if self._initialized:
            logger.info("DatabaseManager already initialized, skipping initialization")
            return True

        logger.info("Starting DatabaseManager initialization with auto-discovery...")

        with log_context(logger, "DatabaseManager initialization", level=20):
            for attempt in range(1, max_retries + 1):
                try:
                    logger.info(f"Connection attempt {attempt}/{max_retries}")

                    success = await self._attempt_connection()
                    if success:
                        if self.auto_discover:
                            await self._auto_discover_databases()

                        await self._verify_databases()
                        self._start_health_monitoring()

                        self._initialized = True
                        self._connection_healthy = True
                        self.metrics["successful_connections"] += 1
                        self.metrics["last_connection_time"] = asyncio.get_event_loop().time()

                        logger.info("✅ DatabaseManager initialization completed successfully")
                        self._log_connection_metrics()
                        return True

                except DatabaseConnectionError as e:
                    self.metrics["failed_connections"] += 1
                    logger.error(f"❌ Connection attempt {attempt} failed: {e}")

                    if attempt < max_retries:
                        logger.info(f"⏳ Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 1.5  # Exponential backoff
                    else:
                        logger.critical(f"💥 All connection attempts failed after {max_retries} retries")
                        raise

                except Exception as e:
                    self.metrics["failed_connections"] += 1
                    logger.error(f"💥 Unexpected error during initialization attempt {attempt}: {e}", exc_info=True)

                    if attempt < max_retries:
                        logger.info(f"⏳ Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 1.5
                    else:
                        logger.critical(f"💥 Initialization failed after {max_retries} attempts")
                        raise DatabaseConnectionError(f"Failed to initialize after {max_retries} attempts") from e

        return False

    async def _attempt_connection(self) -> bool:
        """
        Attempt to establish database connection with comprehensive error handling.

        Returns:
            bool: True if connection successful

        Raises:
            DatabaseConnectionError: If connection fails
        """
        if not ECOM_DATABASE:
            raise DatabaseConnectionError("ECOM_DATABASE environment variable not set")

        logger.debug("Validating MongoDB URI format...")
        if not ECOM_DATABASE.startswith(('mongodb://', 'mongodb+srv://')):
            raise DatabaseConnectionError("Invalid MongoDB URI format")

        self.metrics["connection_attempts"] += 1

        try:
            logger.info("Creating MongoDB client connection...")

            with PerformanceLogger(logger, "mongodb_client_creation"):
                self.db_client = AsyncIOMotorClient(
                    ECOM_DATABASE,
                    maxPoolSize=self.max_pool_size,
                    minPoolSize=self.min_pool_size,
                    maxIdleTimeMS=self.max_idle_time,
                    serverSelectionTimeoutMS=self.server_selection_timeout,
                    connectTimeoutMS=self.connection_timeout,
                    retryWrites=self.retry_writes,
                    retryReads=self.retry_reads,
                    heartbeatFrequencyMS=self.heartbeat_frequency,
                    # Additional production settings
                    maxConnecting=5,
                    waitQueueTimeoutMS=10000,
                    socketTimeoutMS=20000,
                    # Enable monitoring
                    appname="EcomBot-DatabaseManager"
                )

            logger.info("Testing database connection...")

            with PerformanceLogger(logger, "connection_test"):
                # Test the connection
                await self.db_client.admin.command('ping')

            logger.info("✅ Database connection established successfully")
            return True

        except ServerSelectionTimeoutError as e:
            error_msg = f"Server selection timeout - MongoDB server not reachable: {e}"
            logger.error(error_msg)
            raise DatabaseConnectionError(error_msg) from e

        except ConfigurationError as e:
            error_msg = f"MongoDB configuration error: {e}"
            logger.error(error_msg)
            raise DatabaseConnectionError(error_msg) from e

        except OperationFailure as e:
            error_msg = f"MongoDB operation failed - authentication or permission error: {e}"
            logger.error(error_msg)
            raise DatabaseConnectionError(error_msg) from e

        except Exception as e:
            error_msg = f"Unexpected database connection error: {e}"
            logger.error(error_msg, exc_info=True)
            raise DatabaseConnectionError(error_msg) from e

    async def _auto_discover_databases(self):
        """Auto-discover all databases and their collections, creating global mappings."""
        logger.info("🔍 Starting auto-discovery of databases and collections...")

        try:
            with PerformanceLogger(logger, "database_auto_discovery"):
                # Get list of all databases
                database_list = await self.db_client.list_database_names()

                # Filter out system databases
                user_databases = [db for db in database_list if db not in ['admin', 'local', 'config']]
                self.metrics["databases_discovered"] = len(user_databases)

                logger.info(f"📁 Found {len(user_databases)} user databases: {user_databases}")

                for db_name in user_databases:
                    await self._map_database_collections(db_name)

                # Update global mappings
                global DATABASE_MAPPINGS, COLLECTION_REGISTRY
                DATABASE_MAPPINGS.update(self.databases)
                COLLECTION_REGISTRY.update(self._build_collection_registry())

                logger.info(
                    f"✅ Auto-discovery completed: {self.metrics['collections_discovered']} collections mapped across {self.metrics['databases_discovered']} databases")

        except Exception as e:
            logger.error(f"❌ Auto-discovery failed: {e}", exc_info=True)
            raise DatabaseConnectionError(f"Database auto-discovery failed: {e}") from e

    async def _map_database_collections(self, db_name: str):
        """Map all collections for a specific database with enhanced filtering."""
        logger.debug(f"Mapping collections for database: {db_name}")

        database = self.db_client[db_name]
        self.databases[db_name] = database

        try:
            # Get collection info with additional details
            collections_info = await database.list_collections()
            collections = []

            async for collection_info in collections_info:
                collection_name = collection_info['name']

                # Skip system collections
                if collection_name.startswith('system.'):
                    logger.debug(f"  ⏭️  Skipping system collection: {collection_name}")
                    continue

                collections.append(collection_name)

                # Create attribute name in snake_case
                attr_name = f"{db_name.lower()}_{collection_name.lower()}"

                # Store the collection reference
                collection_ref = database[collection_name]
                self.collections[attr_name] = collection_ref

                # Also set as attribute for direct access
                setattr(self, attr_name, collection_ref)

                self.metrics["collections_discovered"] += 1

                logger.debug(f"  📄 Mapped: {db_name}.{collection_name} -> {attr_name}")

            logger.info(f"✅ Database '{db_name}': {len(collections)} collections mapped")

        except Exception as e:
            logger.error(f"❌ Failed to map collections for database '{db_name}': {e}")

    def _build_collection_registry(self) -> Dict[str, Dict[str, Any]]:
        """Build a registry of all collections organized by database."""
        registry = {}

        for attr_name, collection in self.collections.items():
            # Extract database and collection names from attribute name
            parts = attr_name.split('_', 1)
            if len(parts) == 2:
                db_name, coll_name = parts

                if db_name not in registry:
                    registry[db_name] = {}

                registry[db_name][coll_name] = collection

        return registry

    async def _verify_databases(self):
        """Verify databases and collections are accessible."""
        logger.info("Verifying database and collection accessibility...")

        verification_stats = {
            "databases": 0,
            "collections": 0,
            "total_documents": 0
        }

        try:
            for db_name, database in self.databases.items():
                with PerformanceLogger(logger, f"verify_{db_name}"):
                    # Get collections for this database
                    collections = await database.list_collection_names()
                    verification_stats["databases"] += 1
                    verification_stats["collections"] += len(collections)

                    # Sample document count from first collection to verify accessibility
                    if collections:
                        sample_collection = database[collections[0]]
                        count = await sample_collection.estimated_document_count()
                        verification_stats["total_documents"] += count

                    logger.info(f"✅ Database '{db_name}': {len(collections)} collections verified")

        except Exception as e:
            logger.error(f"Database verification failed: {e}", exc_info=True)
            raise DatabaseConnectionError(f"Database verification failed: {e}") from e

        # Log verification summary
        logger.info(f"📊 Verification Summary:")
        logger.info(f"  • Databases: {verification_stats['databases']}")
        logger.info(f"  • Collections: {verification_stats['collections']}")
        logger.info(f"  • Total documents: {verification_stats['total_documents']:,}")

    async def export_mappings(self, file_path: str = None) -> str:
        """
        Export database mappings to JSON file for reference.

        Args:
            file_path: Optional custom file path

        Returns:
            Path to the exported file
        """
        if not self._initialized:
            raise DatabaseOperationError("Database manager not initialized. Call initialize() first.")

        if not file_path:
            file_path = f"Mappings/database_mappings.json"

        mappings = {
            "export_timestamp": datetime.now().isoformat(),
            "databases": {},
            "collections": self._build_collection_registry(),
            "metrics": self.metrics,
            "connection_info": self.get_connection_info()
        }

        # Add database information
        for db_name, database in self.databases.items():
            try:
                collections = await database.list_collection_names()
                mappings["databases"][db_name] = {
                    "collection_count": len(collections),
                    "collections": collections
                }
            except Exception as e:
                logger.error(f"Failed to get info for database {db_name}: {e}")
                mappings["databases"][db_name] = {"error": str(e)}

        # Ensure directory exists
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        # Write to file
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(mappings, f, indent=2, default=str)

        logger.info(f"✅ Database mappings exported to: {file_path}")
        return file_path

    async def load_mappings(self, file_path: str) -> Dict[str, Any]:
        """
        Load database mappings from JSON file.

        Args:
            file_path: Path to mappings file

        Returns:
            Loaded mappings dictionary
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                mappings = json.load(f)

            logger.info(f"✅ Database mappings loaded from: {file_path}")
            return mappings
        except Exception as e:
            logger.error(f"❌ Failed to load mappings from {file_path}: {e}")
            raise DatabaseOperationError(f"Failed to load mappings: {e}") from e

    async def get_collection_stats(self, database_name: str, collection_name: str) -> Dict[str, Any]:
        """
        Get statistics for a specific collection.

        Args:
            database_name: Name of the database
            collection_name: Name of the collection

        Returns:
            Collection statistics
        """
        try:
            collection = self.get_collection(database_name, collection_name)

            stats = {
                "database": database_name,
                "collection": collection_name,
                "document_count": await collection.estimated_document_count(),
                "indexes": await collection.list_indexes().to_list(length=None)
            }

            return stats
        except Exception as e:
            logger.error(f"Failed to get stats for {database_name}.{collection_name}: {e}")
            raise DatabaseOperationError(f"Failed to get collection stats: {e}") from e

    def get_collection(self, database_name: str, collection_name: str) -> Any:
        """
        Get a collection reference by database and collection names.

        Args:
            database_name: Name of the database
            collection_name: Name of the collection

        Returns:
            Collection reference

        Raises:
            DatabaseOperationError: If collection not found
        """
        attr_name = f"{database_name.lower()}_{collection_name.lower()}"

        if attr_name in self.collections:
            return self.collections[attr_name]
        else:
            raise DatabaseOperationError(
                f"Collection '{database_name}.{collection_name}' not found. Available collections: {list(self.collections.keys())}")

    def list_databases(self) -> List[str]:
        """Get list of all discovered databases."""
        return list(self.databases.keys())

    def list_collections(self, database_name: str = None) -> Dict[str, List[str]]:
        """
        Get collections for a specific database or all databases.

        Args:
            database_name: Optional specific database name

        Returns:
            Dictionary mapping database names to list of collections
        """
        if database_name:
            db_key = database_name.lower()
            collections = [attr.split('_', 1)[1] for attr in self.collections.keys() if attr.startswith(f"{db_key}_")]
            return {database_name: collections}
        else:
            result = {}
            for db_name in self.databases:
                db_key = db_name.lower()
                result[db_name] = [attr.split('_', 1)[1] for attr in self.collections.keys() if
                                   attr.startswith(f"{db_key}_")]
            return result

    def _start_health_monitoring(self):
        """Start background health monitoring task"""
        if self._health_check_task and not self._health_check_task.done():
            logger.debug("Health monitoring already running")
            return

        logger.info(f"🔄 Starting database health monitoring (interval: {self.health_check_interval}s)")
        self._health_check_task = asyncio.create_task(self._health_monitor())

    async def _health_monitor(self):
        """Background task to monitor database health"""
        logger.debug("Health monitoring task started")

        try:
            while not self._shutdown_event.is_set():
                try:
                    await asyncio.sleep(self.health_check_interval)

                    if self._shutdown_event.is_set():
                        break

                    await self._perform_health_check()

                except asyncio.CancelledError:
                    logger.info("Health monitoring task cancelled")
                    break
                except Exception as e:
                    logger.error(f"Health check error: {e}", exc_info=True)
                    self.metrics["health_check_failures"] += 1

        except Exception as e:
            logger.error(f"Health monitoring task error: {e}", exc_info=True)
        finally:
            logger.debug("Health monitoring task ended")

    async def _perform_health_check(self):
        """Perform database health check"""
        logger.debug("Performing database health check...")

        try:
            with PerformanceLogger(logger, "health_check"):
                # Ping the database
                await asyncio.wait_for(
                    self.db_client.admin.command('ping'),
                    timeout=5.0
                )

                # Check if we lost connection and regained it
                if not self._connection_healthy:
                    logger.info("✅ Database connection recovered")

                self._connection_healthy = True
                self.metrics["last_health_check"] = asyncio.get_event_loop().time()
                logger.debug("✅ Database health check passed")

        except asyncio.TimeoutError:
            logger.warning("⚠️ Database health check timed out")
            self._connection_healthy = False
            self.metrics["health_check_failures"] += 1

        except Exception as e:
            logger.warning(f"⚠️ Database health check failed: {e}")
            self._connection_healthy = False
            self.metrics["health_check_failures"] += 1

    @asynccontextmanager
    async def operation_context(self, operation_name: str):
        """Context manager for database operations with error tracking"""
        logger.debug(f"Starting database operation: {operation_name}")
        self.metrics["total_operations"] += 1

        try:
            with PerformanceLogger(logger, f"db_operation_{operation_name}"):
                yield
                logger.debug(f"✅ Database operation completed: {operation_name}")
        except Exception as e:
            self.metrics["failed_operations"] += 1
            logger.error(f"❌ Database operation failed: {operation_name} - {e}", exc_info=True)
            raise DatabaseOperationError(f"Operation '{operation_name}' failed: {e}") from e

    async def execute_with_retry(self, operation, operation_name: str, max_retries: int = 3):
        """
        Execute database operation with automatic retry logic

        Args:
            operation: Async function to execute
            operation_name: Name of the operation for logging
            max_retries: Maximum number of retry attempts

        Returns:
            Result of the operation
        """
        logger.debug(f"Executing operation with retry: {operation_name}")

        for attempt in range(1, max_retries + 1):
            try:
                async with self.operation_context(f"{operation_name}_attempt_{attempt}"):
                    result = await operation()

                if attempt > 1:
                    logger.info(f"✅ Operation succeeded after {attempt} attempts: {operation_name}")

                return result

            except Exception as e:
                if attempt < max_retries:
                    logger.warning(f"⚠️ Operation attempt {attempt} failed, retrying: {operation_name}")
                    await asyncio.sleep(0.5 * attempt)  # Progressive delay
                else:
                    logger.error(f"❌ Operation failed after {max_retries} attempts: {operation_name}")
                    raise

        raise DatabaseOperationError(f"Operation '{operation_name}' failed after {max_retries} attempts")

    def _log_connection_metrics(self):
        """Log current connection and performance metrics"""
        logger.info("📊 Database Connection Metrics:")
        logger.info(f"  • Connection attempts: {self.metrics['connection_attempts']}")
        logger.info(f"  • Successful connections: {self.metrics['successful_connections']}")
        logger.info(f"  • Failed connections: {self.metrics['failed_connections']}")
        logger.info(f"  • Reconnection attempts: {self.metrics['reconnection_attempts']}")
        logger.info(f"  • Health check failures: {self.metrics['health_check_failures']}")
        logger.info(f"  • Total operations: {self.metrics['total_operations']}")
        logger.info(f"  • Failed operations: {self.metrics['failed_operations']}")
        logger.info(f"  • Databases discovered: {self.metrics['databases_discovered']}")
        logger.info(f"  • Collections discovered: {self.metrics['collections_discovered']}")

        success_rate = (
            (self.metrics['successful_connections'] / self.metrics['connection_attempts'] * 100)
            if self.metrics['connection_attempts'] > 0 else 0
        )
        logger.info(f"  • Connection success rate: {success_rate:.1f}%")

        if self.metrics['total_operations'] > 0:
            operation_success_rate = (
                ((self.metrics['total_operations'] - self.metrics['failed_operations']) /
                 self.metrics['total_operations'] * 100)
            )
            logger.info(f"  • Operation success rate: {operation_success_rate:.1f}%")

    @log_performance("database_reconnection")
    async def reconnect(self) -> bool:
        """
        Attempt to reconnect to the database

        Returns:
            bool: True if reconnection successful
        """
        logger.info("🔄 Attempting database reconnection...")
        self.metrics["reconnection_attempts"] += 1

        try:
            if self.db_client:
                logger.debug("Closing existing database connection...")
                self.db_client.close()

            self._initialized = False
            self._connection_healthy = False

            # Reinitialize connection
            success = await self.initialize(max_retries=3, retry_delay=1.0)

            if success:
                logger.info("✅ Database reconnection successful")
            else:
                logger.error("❌ Database reconnection failed")

            return success

        except Exception as e:
            logger.error(f"❌ Database reconnection error: {e}", exc_info=True)
            return False

    def is_healthy(self) -> bool:
        """
        Check if database connection is healthy

        Returns:
            bool: True if connection is healthy
        """
        return self._initialized and self._connection_healthy

    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get current connection information

        Returns:
            Dict containing connection status and metrics
        """
        return {
            "initialized": self._initialized,
            "healthy": self._connection_healthy,
            "metrics": self.metrics.copy(),
            "config": {
                "max_pool_size": self.max_pool_size,
                "min_pool_size": self.min_pool_size,
                "connection_timeout": self.connection_timeout,
                "server_selection_timeout": self.server_selection_timeout,
            },
            "databases_count": len(self.databases),
            "collections_count": len(self.collections)
        }

    @log_performance("database_status_check")
    async def get_database_status(self) -> Dict[str, Any]:
        """
        Get comprehensive database status information with discovered databases.

        Returns:
            Dict containing detailed database status
        """
        logger.debug("Gathering database status information...")

        status = {
            "connection": {
                "initialized": self._initialized,
                "healthy": self._connection_healthy,
                "uri_configured": bool(ECOM_DATABASE)
            },
            "metrics": self.metrics.copy(),
            "databases": {},
            "server_info": {}
        }

        if self._initialized and self.db_client:
            try:
                # Get server information
                with PerformanceLogger(logger, "server_status_check"):
                    server_status = await self.db_client.admin.command("serverStatus")
                    status["server_info"] = {
                        "version": server_status.get("version"),
                        "uptime": server_status.get("uptime"),
                        "connections": server_status.get("connections", {})
                    }

                # Get database and collection information
                for db_name, database in self.databases.items():
                    status["databases"][db_name] = {
                        "collections": await database.list_collection_names(),
                        "collection_count": len([c for c in self.collections.keys() if c.startswith(db_name.lower())])
                    }

            except Exception as e:
                logger.error(f"Error gathering database status: {e}")
                status["error"] = str(e)

        logger.debug("Database status information gathered")
        return status

    @log_performance("database_cleanup")
    async def close(self):
        """
        Close database connections with comprehensive cleanup and logging
        """
        logger.info("🔄 Starting database cleanup and connection closure...")

        try:
            with log_context(logger, "database_cleanup", level=20):
                # Signal shutdown to health monitor
                self._shutdown_event.set()

                # Cancel health monitoring task
                if self._health_check_task and not self._health_check_task.done():
                    logger.debug("Cancelling health monitoring task...")
                    self._health_check_task.cancel()

                    try:
                        await asyncio.wait_for(self._health_check_task, timeout=5.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        logger.debug("Health monitoring task cancelled/timed out")

                # Close database client
                if self.db_client:
                    logger.info("Closing MongoDB client connection...")

                    with PerformanceLogger(logger, "mongodb_client_close"):
                        self.db_client.close()

                    logger.info("✅ MongoDB client closed")

                # Reset state
                self.db_client = None
                self.databases.clear()
                self.collections.clear()
                self._initialized = False
                self._connection_healthy = False

                # Clear global mappings
                global DATABASE_MAPPINGS, COLLECTION_REGISTRY
                DATABASE_MAPPINGS.clear()
                COLLECTION_REGISTRY.clear()

                # Log final metrics
                logger.info("📊 Final Database Manager Statistics:")
                self._log_connection_metrics()

        except Exception as e:
            logger.error(f"❌ Error during database cleanup: {e}", exc_info=True)
        finally:
            logger.info("✅ Database cleanup completed")

    def setup_shutdown_handlers(self):
        """Setup signal handlers for graceful shutdown"""

        def signal_handler(signum, frame):
            logger.info(f"📡 Received signal {signum}, initiating graceful shutdown...")
            asyncio.create_task(self.close())

        # Register signal handlers for graceful shutdown
        try:
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            logger.info("📡 Shutdown signal handlers registered")
        except Exception as e:
            logger.warning(f"⚠️ Could not register signal handlers: {e}")

    # Context manager support
    async def __aenter__(self):
        """Async context manager entry"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()


# Global database manager instance with auto-discovery enabled
db_manager = DatabaseManager(auto_discover=True)


# Global access functions for database mappings
def get_database_mappings() -> Dict[str, Any]:
    """Get global database mappings."""
    return DATABASE_MAPPINGS.copy()


def get_collection_registry() -> Dict[str, Dict[str, Any]]:
    """Get global collection registry."""
    return COLLECTION_REGISTRY.copy()


def get_collection(database_name: str, collection_name: str) -> Any:
    """
    Get a collection reference from global mappings.

    Args:
        database_name: Name of the database
        collection_name: Name of the collection

    Returns:
        Collection reference

    Raises:
        DatabaseOperationError: If collection not found
    """
    db_key = database_name.lower()
    coll_key = collection_name.lower()
    attr_name = f"{db_key}_{coll_key}"

    # First try to get from db_manager attributes
    if hasattr(db_manager, attr_name):
        return getattr(db_manager, attr_name)

    # If not found, check if we need to initialize the database manager
    if not db_manager.is_healthy():
        logger.warning(
            f"⚠️ DatabaseManager not healthy, attempting to initialize for collection: {database_name}.{collection_name}")
        # We can't initialize here because it's async, so we'll raise a more helpful error

    # Check if the collection exists in the registry
    global COLLECTION_REGISTRY
    if db_key in COLLECTION_REGISTRY and coll_key in COLLECTION_REGISTRY[db_key]:
        return COLLECTION_REGISTRY[db_key][coll_key]

    # Last resort: try to create the collection reference directly
    try:
        if db_manager.db_client:
            database = db_manager.db_client[database_name]
            collection = database[collection_name]
            logger.warning(f"⚠️ Collection {database_name}.{collection_name} not in mappings, using direct reference")
            return collection
    except Exception as e:
        logger.error(f"❌ Failed to create direct collection reference: {e}")

    raise DatabaseOperationError(
        f"Collection '{database_name}.{collection_name}' not found in global mappings. "
        f"Available databases: {list(db_manager.databases.keys()) if db_manager.databases else 'none'}. "
        f"Available collections: {list(db_manager.collections.keys()) if db_manager.collections else 'none'}"
    )


# Convenience functions for common operations
async def ensure_database_connection() -> bool:
    """
    Ensure database connection is established and healthy

    Returns:
        bool: True if connection is ready
    """
    if not db_manager.is_healthy():
        logger.info("Database not healthy, attempting to initialize/reconnect...")
        return await db_manager.initialize()
    return True


async def get_database_health_status() -> Dict[str, Any]:
    """
    Get current database health status

    Returns:
        Dict containing health status information
    """
    return await db_manager.get_database_status()


@log_performance("database_operation_wrapper")
async def safe_database_operation(operation, operation_name: str, max_retries: int = 3):
    """
    Execute database operation safely with automatic error handling and retries

    Args:
        operation: Async function to execute
        operation_name: Name of the operation for logging
        max_retries: Maximum retry attempts

    Returns:
        Result of the operation

    Raises:
        DatabaseOperationError: If operation fails after all retries
    """
    # Ensure connection is healthy
    if not await ensure_database_connection():
        raise DatabaseConnectionError("Could not establish database connection")

    return await db_manager.execute_with_retry(operation, operation_name, max_retries)


async def export_database_mappings(file_path: str = None) -> str:
    """
    Convenience function to export database mappings to JSON.

    Args:
        file_path: Optional custom file path

    Returns:
        Path to the exported file
    """
    if not await ensure_database_connection():
        raise DatabaseConnectionError("Could not establish database connection")

    return await db_manager.export_mappings(file_path)


async def get_collection_stats(database_name: str, collection_name: str) -> Dict[str, Any]:
    """
    Convenience function to get collection statistics.

    Args:
        database_name: Name of the database
        collection_name: Name of the collection

    Returns:
        Collection statistics
    """
    if not await ensure_database_connection():
        raise DatabaseConnectionError("Could not establish database connection")

    return await db_manager.get_collection_stats(database_name, collection_name)


# Setup graceful shutdown
db_manager.setup_shutdown_handlers()

# Export for convenience
__all__ = [
    'DatabaseManager',
    'DatabaseConnectionError',
    'DatabaseOperationError',
    'db_manager',
    'ensure_database_connection',
    'get_database_health_status',
    'safe_database_operation',
    'get_database_mappings',
    'get_collection_registry',
    'get_collection',
    'export_database_mappings',
    'get_collection_stats'
]