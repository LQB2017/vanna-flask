import os
from odps import ODPS, options

def connect_to_maxcompute(self):
        o = ODPS(
            os.getenv('ACCESSID'),
            os.getenv('ACCESSKEY'),
            project='HOOMI_DWH',
            endpoint='http://service.cn-hangzhou.maxcompute.aliyun.com/api',
        )

        options.sql.settings = {'odps.sql.type.system.odps2': True}
        def run_sql_maxcompute(sql: str):
            import multiprocessing
            n_process = multiprocessing.cpu_count()
            with o.execute_sql(sql).open_reader(tunnel=True) as reader:
                # n_process 指定成机器核数
                pd_df = reader.to_pandas(n_process=n_process)
            return pd_df

        self.run_sql_is_set = True
        self.run_sql = run_sql_maxcompute