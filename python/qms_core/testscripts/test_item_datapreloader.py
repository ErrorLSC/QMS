from qms_core.core.forecast.common.item_data_preloader import ItemDataPreloader
from qms_core.infrastructure.config import MRPConfig
from qms_core.core.item.item import Item

item = Item(itemnum="5697006433",warehouse="6")
config = MRPConfig()
preloader = ItemDataPreloader(config=config,items=[item])

demand_df = preloader.load_demand_history()
# item.demand.load(session=config.get_session(),replacing_map=preloader.replacing_map)

item.demand.load_from_df(demand_df)

item.demand.show_summary()

print(demand_df)


