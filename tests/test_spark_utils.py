"""
Unit tests for Spark utilities module.
"""

import pytest
from unittest.mock import Mock, patch
from src.common.spark_utils import (
    create_spark_session,
    optimize_spark_session,
    get_spark_metrics,
    validate_spark_session
)


class TestSparkUtils:
    """Test cases for Spark utilities."""
    
    @patch('src.common.spark_utils.SparkSession')
    def test_create_spark_session(self, mock_spark_session):
        """Test Spark session creation."""
        mock_session = Mock()
        mock_spark_session.builder.appName.return_value.master.return_value.getOrCreate.return_value = mock_session
        
        session = create_spark_session("TestApp", "local[*]")
        
        assert session == mock_session
        mock_spark_session.builder.appName.assert_called_with("TestApp")
    
    def test_optimize_spark_session(self):
        """Test Spark session optimization."""
        mock_spark = Mock()
        
        optimize_spark_session(mock_spark)
        
        # Verify that configuration was set
        mock_spark.conf.set.assert_called()
    
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
    
    def test_validate_spark_session_success(self):
        """Test successful Spark session validation."""
        mock_spark = Mock()
        mock_df = Mock()
        mock_spark.range.return_value = mock_df
        mock_spark.conf.get.side_effect = lambda key: "true"
        
        result = validate_spark_session(mock_spark)
        
        assert result is True
    
    def test_validate_spark_session_failure(self):
        """Test failed Spark session validation."""
        mock_spark = Mock()
        mock_spark.range.side_effect = Exception("Test error")
        
        result = validate_spark_session(mock_spark)
        
        assert result is False
