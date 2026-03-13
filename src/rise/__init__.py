# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

from copy import deepcopy
import logging

from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from pygeoapi.crs import get_transform_from_spec, get_srid
from pygeoapi.provider.base import ProviderItemNotFoundError
from pygeoapi.provider.sql import GenericSQLProvider

LOGGER = logging.getLogger(__name__)


class RiseProvider(GenericSQLProvider):
    """Rise provider"""

    default_port = 3306

    def __init__(self, provider_def: dict):
        """
        MySQLProvider Class constructor

        :param provider_def: provider definitions from yml pygeoapi-config.
                             data,id_field, name set in parent class
                             data contains the connection information
                             for class DatabaseCursor
        :returns: pygeoapi.provider.sql.MySQLProvider
        """

        driver_name = 'mysql+pymysql'
        extra_conn_args = {'charset': 'utf8mb4'}
        super().__init__(provider_def, driver_name, extra_conn_args)

    def get(self, identifier, crs_transform_spec=None, **kwargs):
        """
        Query the provider for a specific
        feature id e.g: /collections/hotosm_bdi_waterways/items/13990765

        :param identifier: feature id
        :param crs_transform_spec: `CrsTransformSpec` instance, optional

        :returns: GeoJSON FeatureCollection
        """
        LOGGER.debug(f'Get item by ID: {identifier}')

        # Execute query within self-closing database Session context
        with Session(self._engine) as session:
            # Retrieve data from database as feature
            try:
                item = session.get(self.table_model, identifier)
                assert item is not None
                assert getattr(item, self.id_field) == identifier
            except (AssertionError, AttributeError):
                msg = f'No such item: {self.id_field}={identifier}.'
                raise ProviderItemNotFoundError(msg)

            crs_transform_out = get_transform_from_spec(crs_transform_spec)
            feature = self._sqlalchemy_to_feature(item, crs_transform_out)

            # Drop non-defined properties
            if self.properties:
                props = feature['properties']
                dropping_keys = deepcopy(props).keys()
                for item in dropping_keys:
                    if item not in self.properties:
                        props.pop(item)

        return feature

    def _get_bbox_filter(self, bbox: list[float]):
        """
        Construct the bounding box filter function
        """
        if not bbox:
            return True  # Let everything through if no bbox

        # If we are using mysql we can't use ST_MakeEnvelope since it is
        # postgis specific and thus we have to use MBRContains with a WKT
        # POLYGON
        storage_srid = get_srid(self.storage_crs)
        geom_column = func.ST_GeomFromGeoJSON(
            getattr(self.table_model, self.geom)
        )

        # Create WKT POLYGON from bbox: (miny, minx, maxy, maxx)
        miny, minx, maxy, maxx = bbox
        polygon_wkt = f'POLYGON(({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))'  # noqa

        # Use MySQL MBRContains for index-accelerated bounding box checks
        bbox_filter = func.MBRContains(
            func.ST_GeomFromText(polygon_wkt, storage_srid), geom_column
        )

        return bbox_filter
