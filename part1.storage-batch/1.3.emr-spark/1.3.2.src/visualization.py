# 시각화 및 대시보드 함수
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, clear_output
import time
from datetime import datetime

# 시각화 설정
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def visualize_trends(spark_df, date_col, value_col, category_col=None, limit=1000):
    """시계열 트렌드 시각화"""
    
    # 일별 집계
    if category_col:
        daily_stats = spark_df \
            .groupBy(date_col, category_col) \
            .agg(
                sum(value_col).alias("total_value"),
                count("*").alias("count")
            ) \
            .orderBy(date_col)
    else:
        daily_stats = spark_df \
            .groupBy(date_col) \
            .agg(
                sum(value_col).alias("total_value"),
                count("*").alias("count")
            ) \
            .orderBy(date_col)
    
    # Pandas로 변환 (제한된 데이터셋)
    pandas_df = daily_stats.limit(limit).toPandas()
    
    # 시각화
    plt.figure(figsize=(15, 8))
    
    if category_col and category_col in pandas_df.columns:
        # 카테고리별 트렌드
        categories = pandas_df[category_col].unique()
        for i, category in enumerate(categories[:5]):  # 최대 5개 카테고리
            category_data = pandas_df[pandas_df[category_col] == category]
            plt.plot(category_data[date_col], category_data["total_value"], 
                    label=category, marker='o', linewidth=2)
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    else:
        plt.plot(pandas_df[date_col], pandas_df["total_value"], 
                marker='o', linewidth=3, color='#2E86AB')
    
    plt.title(f'{value_col} 시계열 트렌드', fontsize=16, fontweight='bold')
    plt.xlabel('날짜', fontsize=12)
    plt.ylabel('총합', fontsize=12)
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

def create_distribution_plots(spark_df, numeric_cols, sample_size=10000):
    """수치형 변수 분포 시각화"""
    
    # 샘플링
    sample_df = spark_df.sample(fraction=0.1).limit(sample_size).toPandas()
    
    n_cols = len(numeric_cols)
    fig, axes = plt.subplots((n_cols + 1) // 2, 2, figsize=(15, 4 * ((n_cols + 1) // 2)))
    
    if n_cols == 1:
        axes = [axes]
    elif n_cols <= 2:
        axes = axes.flatten()
    else:
        axes = axes.flatten()
    
    for i, col in enumerate(numeric_cols):
        if col in sample_df.columns:
            # 히스토그램과 KDE
            sns.histplot(sample_df[col], kde=True, ax=axes[i])
            axes[i].set_title(f'{col} 분포')
            axes[i].grid(True, alpha=0.3)
    
    # 빈 subplot 제거
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])
    
    plt.tight_layout()
    plt.show()

def create_correlation_heatmap(spark_df, numeric_cols, sample_size=10000):
    """상관관계 히트맵"""
    
    # 샘플링
    sample_df = spark_df.select(numeric_cols).sample(fraction=0.1).limit(sample_size).toPandas()
    
    # 상관관계 계산
    correlation_matrix = sample_df.corr()
    
    # 히트맵
    plt.figure(figsize=(10, 8))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0,
                square=True, fmt='.2f')
    plt.title('변수간 상관관계', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.show()

class RealTimeMonitor:
    """실시간 모니터링 대시보드"""
    
    def __init__(self, spark_session, data_path):
        self.spark = spark_session
        self.data_path = data_path
        self.running = False
    
    def create_dashboard(self):
        """실시간 모니터링 대시보드 생성"""
        
        # 위젯 생성
        self.start_button = widgets.Button(
            description="모니터링 시작",
            button_style='success',
            icon='play'
        )
        self.stop_button = widgets.Button(
            description="중지",
            button_style='danger',
            icon='stop'
        )
        self.refresh_interval = widgets.IntSlider(
            value=10,
            min=5,
            max=60,
            step=5,
            description='갱신 간격(초):',
            style={'description_width': 'initial'}
        )
        self.output = widgets.Output()
        
        # 이벤트 핸들러
        self.start_button.on_click(self.start_monitoring)
        self.stop_button.on_click(self.stop_monitoring)
        
        # 레이아웃
        controls = widgets.HBox([
            self.start_button, 
            self.stop_button, 
            self.refresh_interval
        ])
        dashboard = widgets.VBox([controls, self.output])
        
        display(dashboard)
    
    def start_monitoring(self, button):
        """모니터링 시작"""
        self.running = True
        button.disabled = True
        self.stop_button.disabled = False
        self.monitor_loop()
    
    def stop_monitoring(self, button):
        """모니터링 중지"""
        self.running = False
        self.start_button.disabled = False
        button.disabled = True
    
    def monitor_loop(self):
        """모니터링 루프"""
        while self.running:
            with self.output:
                clear_output(wait=True)
                self.update_metrics()
            time.sleep(self.refresh_interval.value)
    
    def update_metrics(self):
        """메트릭 업데이트"""
        try:
            df = self.spark.read.parquet(self.data_path)
            
            # 실시간 통계
            current_hour = datetime.now().hour
            recent_data = df.filter(hour("timestamp") == current_hour)
            
            total_events = recent_data.count()
            unique_users = recent_data.select("user_id").distinct().count()
            total_value = recent_data.agg(sum("value")).collect()[0][0] or 0
            
            print(f"{'='*50}")
            print(f"실시간 메트릭 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*50}")
            print(f"📊 현재 시간 이벤트: {total_events:,}")
            print(f"👥 활성 사용자: {unique_users:,}")
            print(f"💰 총 거래액: ${total_value:,.2f}")
            
            # 상위 카테고리
            print(f"\n📈 상위 카테고리 (현재 시간):")
            if total_events > 0:
                recent_data.groupBy("category") \
                    .count() \
                    .orderBy(desc("count")) \
                    .show(5, truncate=False)
            else:
                print("   데이터 없음")
            
        except Exception as e:
            print(f"❌ 오류: {e}")

def create_business_dashboard(spark_df):
    """비즈니스 대시보드 생성"""
    
    # 샘플 데이터로 변환
    sample_df = spark_df.sample(0.1).limit(5000).toPandas()
    
    # 대시보드 레이아웃
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. 일별 매출 트렌드
    if 'date' in sample_df.columns and 'value' in sample_df.columns:
        daily_revenue = sample_df.groupby('date')['value'].sum().reset_index()
        ax1.plot(daily_revenue['date'], daily_revenue['value'], marker='o', linewidth=2)
        ax1.set_title('일별 매출 트렌드', fontsize=14, fontweight='bold')
        ax1.tick_params(axis='x', rotation=45)
        ax1.grid(True, alpha=0.3)
    
    # 2. 카테고리별 분포
    if 'category' in sample_df.columns:
        category_dist = sample_df['category'].value_counts()
        ax2.pie(category_dist.values, labels=category_dist.index, autopct='%1.1f%%')
        ax2.set_title('카테고리별 분포', fontsize=14, fontweight='bold')
    
    # 3. 사용자 활동 히트맵
    if 'hour' in sample_df.columns and 'day_of_week' in sample_df.columns:
        heatmap_data = sample_df.pivot_table(
            values='value', 
            index='day_of_week', 
            columns='hour', 
            aggfunc='count',
            fill_value=0
        )
        sns.heatmap(heatmap_data, annot=False, cmap='YlOrRd', ax=ax3)
        ax3.set_title('시간대별 활동 히트맵', fontsize=14, fontweight='bold')
    
    # 4. 상위 사용자
    if 'user_id' in sample_df.columns and 'value' in sample_df.columns:
        top_users = sample_df.groupby('user_id')['value'].sum().nlargest(10)
        ax4.barh(range(len(top_users)), top_users.values)
        ax4.set_yticks(range(len(top_users)))
        ax4.set_yticklabels([f'User {uid}' for uid in top_users.index])
        ax4.set_title('상위 10 사용자 (매출 기준)', fontsize=14, fontweight='bold')
        ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()

# 인터랙티브 필터링 위젯
def create_interactive_filter(spark_df):
    """인터랙티브 데이터 필터링 위젯"""
    
    # 카테고리 옵션 수집
    categories = [row[0] for row in spark_df.select("category").distinct().collect()]
    
    # 위젯 생성
    category_filter = widgets.SelectMultiple(
        options=categories,
        value=categories[:3] if len(categories) >= 3 else categories,
        description='카테고리:',
        style={'description_width': 'initial'}
    )
    
    date_range = widgets.SelectionRangeSlider(
        options=[f'2024-01-{i:02d}' for i in range(1, 32)],
        index=(0, 7),
        description='날짜 범위:',
        style={'description_width': 'initial'}
    )
    
    value_threshold = widgets.FloatSlider(
        value=0.0,
        min=0.0,
        max=1000.0,
        step=10.0,
        description='최소 금액:',
        style={'description_width': 'initial'}
    )
    
    output = widgets.Output()
    
    def update_plot(change):
        with output:
            clear_output(wait=True)
            
            # 필터링 적용
            filtered_df = spark_df.filter(
                col("category").isin(list(category_filter.value)) &
                (col("value") >= value_threshold.value)
            )
            
            # 결과 표시
            print(f"필터링 결과: {filtered_df.count():,} 레코드")
            filtered_df.groupBy("category").agg(
                count("*").alias("count"),
                sum("value").alias("total_value")
            ).show()
    
    # 이벤트 핸들러 연결
    category_filter.observe(update_plot, names='value')
    value_threshold.observe(update_plot, names='value')
    
    # 초기 플롯 생성
    update_plot(None)
    
    # 레이아웃
    controls = widgets.VBox([category_filter, date_range, value_threshold])
    dashboard = widgets.HBox([controls, output])
    
    display(dashboard)

if __name__ == "__main__":
    print("✅ 시각화 함수들이 로드되었습니다.")
    print("   - visualize_trends(): 시계열 트렌드")
    print("   - create_distribution_plots(): 분포 시각화")
    print("   - create_correlation_heatmap(): 상관관계 히트맵")
    print("   - RealTimeMonitor(): 실시간 모니터링")
    print("   - create_business_dashboard(): 비즈니스 대시보드")
    print("   - create_interactive_filter(): 인터랙티브 필터")
