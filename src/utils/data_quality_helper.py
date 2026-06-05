# ============================================================
# FILE: data_quality_helper.py
# MỤC ĐÍCH: Helper class định nghĩa bộ quy tắc và thực hiện kiểm định
#            chất lượng dữ liệu (Data Quality) bằng Great Expectations 1.x
# ============================================================

import logging
from typing import Any, Dict, List

import great_expectations as gx
import pandas as pd
from great_expectations.expectations import (
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToNotBeNull,
)

logger = logging.getLogger("airflow.task")

class DataQualityChecker:
    """Lớp hỗ trợ kiểm định chất lượng dữ liệu cho dự án TradeStream.

    Sử dụng EphemeralDataContext của Great Expectations 1.x để kiểm tra dữ liệu
    trong bộ nhớ (Pandas DataFrame) một cách linh hoạt mà không cần tạo file cấu hình.
    """

    def __init__(self, datasource_name: str = "tradestream_datasource", suite_name: str = "tradestream_quality_suite") -> None:
        """Khởi tạo DataQualityChecker.

        Args:
            datasource_name (str): Tên nguồn dữ liệu sử dụng trong context.
            suite_name (str): Tên bộ quy chuẩn kiểm định chất lượng.
        """
        self.datasource_name: str = datasource_name
        self.suite_name: str = suite_name
        self.context = gx.get_context()

    def run_validation(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Thực hiện kiểm định DataFrame dựa trên bộ quy tắc đã được định nghĩa.

        Bộ quy tắc bao gồm:
        1. Cột 'symbol' không được chứa giá trị NULL (mostly=1.0).
        2. Cột 'close_price' tỷ lệ bị NULL không được vượt quá 1% (mostly=0.99).
        3. Cột 'close_price' phải là số dương (> 0).
        4. Cột 'open_price' phải là số dương (> 0).
        5. Cột 'volume' phải lớn hơn hoặc bằng 0.

        Args:
            df (pd.DataFrame): DataFrame chứa dữ liệu giao dịch cần kiểm định.

        Returns:
            Dict[str, Any]: Kết quả kiểm định gồm:
                - 'success' (bool): True nếu vượt qua tất cả các bài kiểm tra.
                - 'failed_rules' (List[Dict[str, Any]]): Chi tiết các quy tắc bị thất bại.
                - 'summary' (str): Chuỗi tóm tắt kết quả kiểm định.

        Raises:
            ValueError: Nếu DataFrame đầu vào trống.
        """
        if df.empty:
            raise ValueError("Không thể kiểm định dữ liệu vì DataFrame rỗng.")

        logger.info(f"Đang thực hiện kiểm định chất lượng cho {len(df)} bản ghi...")

        # 1. Khởi tạo datasource và asset tạm thời
        try:
            datasource = self.context.data_sources.add_pandas(name=self.datasource_name)
        except Exception:
            datasource = self.context.data_sources.get(self.datasource_name)

        try:
            data_asset = datasource.add_dataframe_asset(name="quality_asset")
        except Exception:
            data_asset = datasource.get_asset("quality_asset")

        try:
            batch_definition = data_asset.add_batch_definition_whole_dataframe(name="quality_batch_def")
        except Exception:
            batch_definition = data_asset.get_batch_definition("quality_batch_def")

        # 2. Khởi tạo bộ quy tắc Expectation Suite
        try:
            suite = self.context.suites.add(gx.ExpectationSuite(self.suite_name))
        except Exception:
            suite = self.context.suites.get(self.suite_name)

        # Xóa các expectation cũ nếu có để tránh lặp lại
        suite.expectations.clear()

        # 3. Định nghĩa các quy tắc kiểm tra (Expectations)
        # 3.1. Symbol không được NULL
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="symbol", mostly=1.0))
        # 3.2. Close price không được NULL quá 1%
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="close_price", mostly=0.99))
        # 3.3. Close price > 0
        suite.add_expectation(ExpectColumnValuesToBeBetween(column="close_price", min_value=0.000001, mostly=1.0))
        # 3.4. Open price > 0
        suite.add_expectation(ExpectColumnValuesToBeBetween(column="open_price", min_value=0.000001, mostly=1.0))
        # 3.5. Volume >= 0
        suite.add_expectation(ExpectColumnValuesToBeBetween(column="volume", min_value=0.0, mostly=1.0))

        # 4. Tạo Validation Definition
        validation_name = "quality_validation_def"
        try:
            validation_definition = self.context.validation_definitions.add(
                gx.ValidationDefinition(name=validation_name, data=batch_definition, suite=suite)
            )
        except Exception:
            validation_definition = self.context.validation_definitions.get(validation_name)

        # 5. Khởi tạo Checkpoint và chạy Validation
        checkpoint_name = "quality_checkpoint"
        try:
            checkpoint = self.context.checkpoints.add(
                gx.Checkpoint(name=checkpoint_name, validation_definitions=[validation_definition])
            )
        except Exception:
            checkpoint = self.context.checkpoints.get(checkpoint_name)

        result = checkpoint.run(batch_parameters={"dataframe": df})

        # 6. Tổng hợp kết quả chi tiết
        failed_rules: List[Dict[str, Any]] = []
        validation_result_details = result.run_results[list(result.run_results.keys())[0]]

        for rule_res in validation_result_details.results:
            if not rule_res.success:
                expectation_config = rule_res.expectation_config
                expectation_type = getattr(expectation_config, "type", getattr(expectation_config, "expectation_type", "unknown"))
                kwargs = expectation_config.kwargs
                observed = rule_res.result.get("observed_value")
                failed_rules.append({
                    "expectation_type": expectation_type,
                    "column": kwargs.get("column"),
                    "mostly": kwargs.get("mostly", 1.0),
                    "observed_value": observed,
                    "details": rule_res.result
                })

        success: bool = result.success
        total_rules = len(validation_result_details.results)
        passed_rules = total_rules - len(failed_rules)

        summary_msg = f"Kết quả kiểm định chất lượng: {'Thành công' if success else 'Thất bại'}. Đạt {passed_rules}/{total_rules} tiêu chí."
        if failed_rules:
            summary_msg += f" Chi tiết lỗi: {failed_rules}"

        logger.info(summary_msg)

        return {
            "success": success,
            "failed_rules": failed_rules,
            "summary": summary_msg
        }
