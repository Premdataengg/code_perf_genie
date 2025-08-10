"""
Unit tests for Spark utilities module.
"""

import pytest
from unittest.mock import Mock, patch

# Try to import PySpark, but don't fail if it's not available
try:
    from src.common.spark_utils import (
        create_spark_session,
        optimize_spark_session,
        get_spark_metrics,
        validate_spark_session
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    # Create dummy functions for testing
    def create_spark_session(*args, **kwargs):
        raise ImportError("PySpark not available")
    
    def optimize_spark_session(*args, **kwargs):
        raise ImportError("PySpark not available")
    
    def get_spark_metrics(*args, **kwargs):
        raise ImportError("PySpark not available")
    
    def validate_spark_session(*args, **kwargs):
        raise ImportError("PySpark not available")


class TestSparkUtils:
    """Test cases for Spark utilities."""
    
    @pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
    @patch('src.common.spark_utils.SparkSession')
    def test_create_spark_session(self, mock_spark_session):
        """Test Spark session creation."""
        # Create a chain of mocks that return themselves for method chaining
        mock_builder = Mock()
        mock_app_name = Mock()
        mock_master = Mock()
        mock_config = Mock()
        mock_get_or_create = Mock()
        mock_session = Mock()
        
        # Set up the chain: builder -> appName -> master -> config -> config -> ... -> getOrCreate
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_app_name
        mock_app_name.master.return_value = mock_master
        mock_master.config.return_value = mock_config
        mock_config.config.return_value = mock_config  # config calls return the same object
        mock_config.getOrCreate.return_value = mock_session
        
        session = create_spark_session("TestApp", "local[*]")
        
        assert session == mock_session
        mock_builder.appName.assert_called_with("TestApp")
        mock_app_name.master.assert_called_with("local[*]")
        # Verify that config was called (multiple times)
        assert mock_master.config.call_count > 0
    
    @pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
    @patch('src.common.spark_utils.SparkSession')
    def test_create_spark_session_with_custom_config(self, mock_spark_session):
        """Test Spark session creation with custom configuration."""
        # Create a chain of mocks that return themselves for method chaining
        mock_builder = Mock()
        mock_app_name = Mock()
        mock_master = Mock()
        mock_config = Mock()
        mock_session = Mock()
        
        # Set up the chain
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_app_name
        mock_app_name.master.return_value = mock_master
        mock_master.config.return_value = mock_config
        mock_config.config.return_value = mock_config
        mock_config.getOrCreate.return_value = mock_session
        
        custom_config = {"spark.sql.shuffle.partitions": "100"}
        session = create_spark_session("TestApp", "local[*]", custom_config)
        
        assert session == mock_session
        # Verify that custom config was applied
        assert mock_config.config.call_count > 0
    
    @pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
    def test_optimize_spark_session(self):
        """Test Spark session optimization."""
        mock_spark = Mock()
        
        optimize_spark_session(mock_spark)
        
        # Verify that configuration was set
        mock_spark.conf.set.assert_called()
    
    @pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
    def test_get_spark_metrics(self):
        """Test getting Spark metrics."""
        mock_spark = Mock()
        mock_spark.version = "3.5.6"
        mock_spark.conf.get.side_effect = lambda key: {
            "spark.master": "local[*]",
            "spark.app.name": "TestApp",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.autoBroadcastJoinThreshold": "10m"
        }.get(key)
        
        metrics = get_spark_metrics(mock_spark)
        
        assert metrics["spark_version"] == "3.5.6"
        assert metrics["master_url"] == "local[*]"
        assert metrics["app_name"] == "TestApp"
    
    @pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
    def test_validate_spark_session_success(self):
        """Test successful Spark session validation."""
        mock_spark = Mock()
        mock_df = Mock()
        mock_spark.range.return_value = mock_df
        mock_spark.conf.get.side_effect = lambda key: "true"
        
        result = validate_spark_session(mock_spark)
        
        assert result is True
    
    @pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not available")
    def test_validate_spark_session_failure(self):
        """Test failed Spark session validation."""
        mock_spark = Mock()
        mock_spark.range.side_effect = Exception("Test error")
        
        result = validate_spark_session(mock_spark)
        
        assert result is False
