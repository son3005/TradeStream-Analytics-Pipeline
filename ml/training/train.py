import os
import sys
import logging
import pandas as pd
import numpy as np
import mlflow
import mlflow.xgboost
from mlflow.tracking import MlflowClient
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

# Thêm đường dẫn root vào sys.path để import ml
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from ml.features.build_features import get_historical_data, calculate_features

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("train")

def main() -> None:
    """
    Quy trình chính chạy huấn luyện mô hình XGBoost, tính toán metrics, log lên MLflow và đăng ký mô hình lên Registry.
    
    Args:
        Không có tham số đầu vào.
        
    Returns:
        None.
        
    Raises:
        Exception: Lỗi phát sinh trong quá trình chuẩn bị dữ liệu, huấn luyện hoặc đăng ký mô hình lên MLflow.
    """
    logger.info("Đang khởi chạy quy trình huấn luyện mô hình...")
    
    # 1. Lấy dữ liệu và tính toán đặc trưng
    try:
        raw_df: pd.DataFrame = get_historical_data()
        df: pd.DataFrame = calculate_features(raw_df, is_training=True)
    except Exception as e:
        logger.error(f"Lỗi khi chuẩn bị dữ liệu: {e}")
        sys.exit(1)
        
    if len(df) == 0:
        logger.error("Dữ liệu rỗng! Không thể huấn luyện.")
        sys.exit(1)
        
    # 2. Cấu hình đặc trưng đầu vào và nhãn
    features: list[str] = ['sma_5', 'sma_10', 'sma_20', 'close_lag_1', 'volume_lag_1', 'return_lag_1']
    target: str = 'target'
    
    # Sắp xếp theo ngày để chia dữ liệu theo thời gian (ngăn rò rỉ dữ liệu)
    df = df.sort_values(by='full_date').reset_index(drop=True)
    
    # Chia Train/Test theo tỷ lệ 80/20 (giữ nguyên thứ tự thời gian)
    split_idx: int = int(len(df) * 0.8)
    train_df: pd.DataFrame = df.iloc[:split_idx]
    test_df: pd.DataFrame = df.iloc[split_idx:]
    
    X_train: pd.DataFrame = train_df[features]
    y_train: pd.Series = train_df[target]
    X_test: pd.DataFrame = test_df[features]
    y_test: pd.Series = test_df[target]
    
    logger.info(f"Kích thước tập huấn luyện (Train): {X_train.shape}")
    logger.info(f"Kích thước tập kiểm thử (Test): {X_test.shape}")
    
    # 3. Cấu hình MLflow Tracking
    # MLflow server chạy nội bộ docker là http://mlflow:5000
    mlflow_uri: str = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("price_direction_experiment")
    
    # 4. Huấn luyện mô hình với XGBoost
    max_depth: int = 5
    learning_rate: float = 0.05
    n_estimators: int = 100
    
    logger.info(f"Bắt đầu huấn luyện XGBClassifier (max_depth={max_depth}, lr={learning_rate}, n_estimators={n_estimators})...")
    
    with mlflow.start_run() as run:
        model: XGBClassifier = XGBClassifier(
            max_depth=max_depth,
            learning_rate=learning_rate,
            n_estimators=n_estimators,
            random_state=42,
            eval_metric="logloss"
        )
        
        model.fit(X_train, y_train)
        
        # 5. Đánh giá mô hình
        y_pred: np.ndarray = model.predict(X_test)
        
        acc: float = accuracy_score(y_test, y_pred)
        prec: float = precision_score(y_test, y_pred, zero_division=0)
        rec: float = recall_score(y_test, y_pred, zero_division=0)
        f1: float = f1_score(y_test, y_pred, zero_division=0)
        
        logger.info(f"[METRIC] Accuracy: {acc:.4f}")
        logger.info(f"[METRIC] Precision: {prec:.4f}")
        logger.info(f"[METRIC] Recall: {rec:.4f}")
        logger.info(f"[METRIC] F1-Score: {f1:.4f}")
        
        # 6. Log tham số và độ đo lên MLflow
        mlflow.log_param("max_depth", max_depth)
        mlflow.log_param("learning_rate", learning_rate)
        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_param("features", ",".join(features))
        
        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("precision", prec)
        mlflow.log_metric("recall", rec)
        mlflow.log_metric("f1_score", f1)
        
        # Log model và đăng ký vào Model Registry
        model_name: str = "price_direction_model"
        logger.info(f"Đang lưu và đăng ký mô hình '{model_name}' vào MLflow Registry...")
        
        # log_model trả về thông tin model đã lưu
        mlflow.xgboost.log_model(
            xgb_model=model,
            artifact_path="model",
            registered_model_name=model_name
        )
        
        # 7. So sánh mô hình mới (Challenger) với mô hình Production hiện tại (Champion)
        client: MlflowClient = MlflowClient()
        
        # 7.1 Lấy thông tin mô hình Production hiện tại
        champion_acc: float = 0.0
        champion_version: str = ""
        try:
            # Lấy các phiên bản mô hình đang ở stage "Production"
            production_versions = client.get_latest_versions(model_name, stages=["Production"])
            if production_versions:
                champion_version = production_versions[0].version
                champion_uri: str = f"models:/{model_name}/Production"
                champion_model = mlflow.xgboost.load_model(champion_uri)
                
                # Dự đoán trên tập kiểm thử (Test set) bằng mô hình cũ để so sánh
                y_pred_champion = champion_model.predict(X_test)
                champion_acc = accuracy_score(y_test, y_pred_champion)
                logger.info(f"🏆 Mô hình Production hiện tại (Champion v{champion_version}) đạt Accuracy: {champion_acc:.4f}")
            else:
                logger.info("ℹ️ Chưa có mô hình nào ở trạng thái Production.")
        except Exception as e:
            logger.warning(f"Không thể nạp mô hình Production hiện tại để so sánh: {e}. Coi như chưa có mô hình cũ.")

        # 7.2 Lấy thông tin phiên bản vừa đăng ký (Challenger)
        latest_versions = client.get_latest_versions(model_name, stages=["None"])
        if latest_versions:
            latest_version: str = latest_versions[0].version
            logger.info(f"⚡ Mô hình mới huấn luyện (Challenger v{latest_version}) đạt Accuracy: {acc:.4f}")
            
            # Chỉ chuyển giao Stage sang Production nếu mô hình mới tốt hơn hoặc bằng mô hình cũ
            # (Hoặc nếu chưa có mô hình nào ở Production)
            if champion_version == "" or acc > champion_acc:
                logger.info(f"🎉 Challenger v{latest_version} chiến thắng! Tiến hành cập nhật lên Production...")
                client.transition_model_version_stage(
                    name=model_name,
                    version=latest_version,
                    stage="Production",
                    archive_existing_versions=True
                )
                logger.info(f"[SUCCESS] Đã chuyển phiên bản v{latest_version} sang trạng thái 'Production'.")
            else:
                logger.info(f"❌ Challenger v{latest_version} không tốt hơn Champion v{champion_version} ({acc:.4f} <= {champion_acc:.4f}). Giữ nguyên mô hình Production hiện tại.")
        else:
            logger.warning("Không tìm thấy phiên bản mô hình nào vừa đăng ký.")

if __name__ == "__main__":
    main()

