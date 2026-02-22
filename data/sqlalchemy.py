# from requests import session
from pathlib import Path
import pandas as pd
from dataclasses import dataclass

from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, Integer, 
    String, DateTime, Float, 
    Boolean, inspect, 
    select, text, create_engine, true)

from utils.logger import LoggerSingleton
log = LoggerSingleton().get_logger()

Base = declarative_base()

@dataclass
class DBStatus:
    exists: bool
    can_connect: bool
    tables_ok: bool
    # missing_tables: list[str]
    has_data: bool | None  

class DBManager():
    def __init__(
            self, 
            db_name="studyanalytics.db", 
            db_path:str|Path=Path(__file__).resolve().parent.parent
        ):
        self.db_file = db_path / db_name
        self.engine = create_engine(f"sqlite:///{db_path}/{db_name}", echo=False)
        self.session = sessionmaker(bind=self.engine)

    def inspect_status(self)->DBStatus:
        exists = self.db_file.exists()
        
        try:
            insp = inspect(self.engine)
            tables = set(insp.get_table_names())
            
            has_tables = False
            if len(tables) > 0: has_tables = True

            has_data = False
            if has_tables == True:
                with self.engine.connect() as conn:
                    n = conn.execute(text("SELECT 1 FROM main_data LIMIT 1")).fetchone()
                    has_data = (n is not None)
    
            return DBStatus(
                exists=exists,
                can_connect=True,
                tables_ok=has_tables,
                has_data=has_data
            )
        
        except SQLAlchemyError:
            return DBStatus(
                exists=exists,
                can_connect=False,
                tables_ok=False,
                has_data=None
            )
        
    def createTables(self):
        log.debug("Starting database")
        Base.metadata.create_all(self.engine)

    def insert_to_main_data(self, df: pd.DataFrame):
        '''
        Used for bulk insert of data into table.
        This is not to be used on updates, as this leads to duplication of data.
        Use upsert function instead.
        '''
        log.debug("Inserting to main_data table.")
        session = self.session()
        try:
            records = df.to_dict(orient='records')
            session.bulk_insert_mappings(MainDataTable, records)
            session.commit()
            log.debug(f"Inserted a total of {len(records)} to db.")
        except:
            session.rollback()
            log.error("Error while trying to insert dataframe into main_data table"
                      f"\n\tdataframe:\n{df.info}\n")
            raise
        finally:
            session.close() 
    
    def upsert_to_tables(self, table:str, df: pd.DataFrame):
        '''
        Accepts 'main', 'daily', 'weekly' for tables.
        '''
        session = self.session()

        TABLE_MAP = {
            'main': MainDataTable,
            'daily': DailyDataTable,
            # 'weekly': WeeklyDataTable,
        }
        tbl = TABLE_MAP[table]

        try:
            for record in df.to_dict(orient="records"):
                # Create a transient instance…
                obj = tbl(**record)
                # …then merge() will INSERT if no PK/unique key match exists,
                # or UPDATE the existing row otherwise.
                session.merge(obj)

                # log.debug(f"Upsert to db {record}")

            session.commit()
        except:
            session.rollback()
            log.error(f"Error occurring while tying to upsert into table {table}")
            raise
        finally:
            session.close()

    def change_auto_exclude(self, course:str, period:str, auto_exclude:str|list|None):
        session = self.session()
        try:
            stmt = select(PeriodDataTable).where(
                PeriodDataTable.course == course,
                PeriodDataTable.period == period,
            )
            row = session.execute(stmt).scalar_one_or_none()

            if row is None:
                raise ValueError(f"No period_data row found for course={course!r}, period={period!r}")
            
            if isinstance(auto_exclude, list): auto_exclude = str(auto_exclude)
            row.auto_exclude = auto_exclude
            
            session.commit()
            return row  # optional: return updated ORM object
        except:
            session.rollback()
            log.error("Error while trying to update auto_exclude in period_data table")
            raise
        finally:
            session.close()

    def insert_period_data(self, 
            course:str, 
            period:str, 
            start_date:DateTime, 
            auto_exclude:String|list|None=None, 
            finished:bool = True
        ):
        log.debug("Inserting to period_data table.")
        
        if auto_exclude is not None:
            if isinstance(auto_exclude, list) and auto_exclude == "[]":
                auto_exclude = None
            else: auto_exclude = str(auto_exclude)

        session = self.session()
        try:
            session.add(PeriodDataTable(
                course=course,
                period=period,
                start_date=start_date,
                finished=finished,
                auto_exclude=auto_exclude
            ))
            session.commit()
        except:
            session.rollback()
            log.error("Error while trying to insert into period_data table")
            raise
        finally:
            session.close() 

    def insert_daily_data(self, df: pd.DataFrame):
        log.debug("Inserting to daily_data table.")
        session = self.session()
        try:
            records = df.to_dict(orient='records')
            session.bulk_insert_mappings(DailyDataTable, records)
            session.commit()
        except:
            session.rollback()
            log.error("Error while trying to insert dataframe into daily_data table"
                      f"\n\tdataframe:\n{df.info}\n")
            raise
        finally:
            session.close()

    '''
    def insert_weekly_data(self, df: pd.DataFrame):
        log.debug("Inserting to weekly_data table.")
        session = self.session()
        try:
            records = df.to_dict(orient='records')
            session.bulk_insert_mappings(WeeklyDataTable, records)
            session.commit()
        except:
            session.rollback()
            log.error("Error while trying to insert dataframe into weekly_data table"
                      f"\n\tdataframe:\n{df.info}\n")
            raise
        finally:
            session.close()
    '''

    def get_period_data(self, 
        course:str, 
        period:str, 
    )->dict:
        session = self.session()
        query = session.query(PeriodDataTable)
        if course is not None:
            query = query.filter(PeriodDataTable.course == course)
        if period is not None:
            query = query.filter(PeriodDataTable.period == period)
        results = query.all()

        records = {
            "id":           results[0].id,
            "course":       results[0].course,
            "period":       results[0].period,
            "start_date":   results[0].start_date,
            "finished":     results[0].finished,
            "auto_exclude": convert_str_list_to_list(results[0].auto_exclude)
        }
        
        return records

    def get_main_data(self,
        course:str | None = None, 
        period:str | None = None, 
        subject:str | None = None
    )-> pd.DataFrame:
        session = self.session()
        query = session.query(MainDataTable)

        if course is not None:
            query = query.filter(MainDataTable.course == course)
        if period is not None:
            query = query.filter(MainDataTable.period == period)
        if subject is not None:
            query = query.filter(MainDataTable.subject == subject)

        results = query.all()

        records = [
            {
                "course":           row.course,
                "period":           row.period,
                "subject":          row.subject,
                "task_name":        row.task_name,
                "start_time":       row.start_time,
                "end_time":         row.end_time,
                "time_spent_hrs":   row.time_spent_hrs,
                "finished":         row.finished
            }
            for row in results
        ]
        
        df = pd.DataFrame.from_records(records)
        df["start_time"] = pd.to_datetime(df["start_time"])
        df["end_time"] = pd.to_datetime(df["end_time"])

        return df

    def get_daily_data(self,
        course: str | None = None,
        period: str | None = None,
        subject: str | None = None
    ) -> pd.DataFrame:
        session = self.session()
        query = session.query(DailyDataTable)

        if course is not None:
            query = query.filter(DailyDataTable.course == course)
        if period is not None:
            query = query.filter(DailyDataTable.period == period)
        if subject is not None:
            query = query.filter(DailyDataTable.subject == subject)

        results = query.all()

        records = [
            {
                # "id":       row.id,
                "date":     row.date,
                "course":   row.course,
                "period":   row.period,
                "subject":  row.subject,
                "time_spent_hrs": row.time_spent_hrs
            }
            for row in results
        ]
        df = pd.DataFrame.from_records(records)
        df["date"] = pd.to_datetime(df["date"])

        return df

    def get_subjects(self, course=None, period=None):
        with self.session() as s:
            # Start from distinct subjects
            q = s.query(MainDataTable.subject).distinct()

            # Apply optional filters
            if course is not None:
                q = q.filter(MainDataTable.course == course)
            if period is not None:
                q = q.filter(MainDataTable.period == period)

            # Order for stable, human-friendly output
            q = q.order_by(MainDataTable.subject.asc())

            # `q.all()` returns a list of 1-tuples when selecting a single column
            subjects = [row[0] for row in q.all()]
        log.debug(f"Extracted from db subjects from course={course}, period={period}")
        return subjects

class MainDataTable(Base):
    __tablename__ = 'main_data'
    course          = Column(String, 
                        primary_key=True)
    period          = Column(String, 
                        primary_key=True)
    subject         = Column(String, 
                        primary_key=True)
    task_name       = Column(String, 
                        primary_key=True)
    start_time      = Column(DateTime(timezone=True), 
                        primary_key=True)
    end_time        = Column(DateTime(timezone=True))
    time_spent_hrs  = Column(Float)
    finished        = Column(Boolean, default=False)

class PeriodDataTable(Base):
    __tablename__   = 'period_data'
    id              = Column(Integer, 
                        primary_key=True, 
                        autoincrement=True)
    course          = Column(String)
    period          = Column(String)
    start_date      = Column(DateTime(timezone=True))
    # end_date        = Column(DateTime(timezone=True), nullable=True, default=None)
    finished        = Column(Boolean, default=True)
    auto_exclude    = Column(String, nullable=True)

class DailyDataTable(Base):
    __tablename__ = 'daily_data'
    date            = Column(DateTime(timezone=True), 
                        primary_key=True)
    course          = Column(String, 
                        primary_key=True)
    period          = Column(String, 
                        primary_key=True)
    subject         = Column(String, 
                        primary_key=True,nullable=True)
    time_spent_hrs  = Column(Float)


def convert_str_list_to_list(str_list: str | list | None) -> list[str]:
    import ast
    if str_list is None:
        return []
    
    if isinstance(str_list, list):
        return [str(x).strip() for x in str_list if str(x).strip()]

    s = str(str_list).strip()
    if s == "" or s == "[]":
        return []
    
    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except (ValueError, SyntaxError):
            pass  # fall through

    # Comma-separated fallback
    return [x.strip().strip("'\"") for x in s.split(",") if x.strip().strip("'\"")]


''' Removed until implementation
class WeeklyDataTable(Base):
    __tablename__ = 'weekly_data'
    id              = Column(Integer, 
                        primary_key=True)
    course          = Column(String)
    period          = Column(String)
    subject         = Column(String)
    week_number     = Column(Integer)
    week            = Column(String)
    time_spent_hrs  = Column(Float)
'''