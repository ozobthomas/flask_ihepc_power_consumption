from sqlalchemy import Column, Integer, Date, DateTime, Float, Index, UniqueConstraint
from ihepcdatabase import Base

class PowerConsumption(Base):
    __tablename__ = "power_consumption"
    __table_args__ = {'sqlite_autoincrement': True}
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    date_time = Column(DateTime, nullable=False)
    global_active_power = Column(Float)
    global_reactive_power = Column(Float)
    voltage = Column(Float)
    global_intensity = Column(Float)
    sub_metering_1 = Column(Float)
    sub_metering_2 = Column(Float)
    sub_metering_3 = Column(Float)
    line_no = Column(Integer)
    UniqueConstraint('date_time', name='power_date_time_unq')
    Index('power_date_time_idx', 'date_time')
    Index('power_date_idx', 'date')
