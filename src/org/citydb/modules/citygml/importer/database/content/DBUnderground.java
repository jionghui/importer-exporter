/*
 * 3D City Database - The Open Source CityGML Database
 * http://www.3dcitydb.org/
 * 
 * Copyright 2013 - 2016
 * Chair of Geoinformatics
 * Technical University of Munich, Germany
 * https://www.gis.bgu.tum.de/
 * 
 * The 3D City Database is jointly developed with the following
 * cooperation partners:
 * 
 * virtualcitySYSTEMS GmbH, Berlin <http://www.virtualcitysystems.de/>
 * M.O.S.S. Computer Grafik Systeme GmbH, Taufkirchen <http://www.moss.de/>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.citydb.modules.citygml.importer.database.content;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.citydb.api.geometry.GeometryObject;
import org.citydb.database.TableEnum;
import org.citydb.log.Logger;
import org.citydb.modules.citygml.common.database.xlink.DBXlinkBasic;
import org.citydb.modules.citygml.common.database.xlink.DBXlinkSurfaceGeometry;
import org.citydb.util.Util;
import org.citygml4j.model.citygml.CityGMLClass;
import org.citygml4j.model.citygml.underground.AbstractBoundarySurface;
import org.citygml4j.model.citygml.underground.AbstractUnderground;
import org.citygml4j.model.citygml.underground.BoundarySurfaceProperty;
import org.citygml4j.model.citygml.underground.UndergroundPart;
import org.citygml4j.model.citygml.underground.UndergroundPartProperty;
import org.citygml4j.model.gml.basicTypes.DoubleOrNull;
import org.citygml4j.model.gml.basicTypes.MeasureOrNullList;
import org.citygml4j.model.gml.geometry.aggregates.MultiCurveProperty;
import org.citygml4j.model.gml.geometry.aggregates.MultiSurfaceProperty;
import org.citygml4j.model.gml.geometry.primitives.SolidProperty;

public class DBUnderground implements DBImporter {
	private final Logger LOG = Logger.getInstance();

	private final Connection batchConn;
	private final DBImporterManager dbImporterManager;

	private PreparedStatement psUnderground;
	private DBCityObject cityObjectImporter;
	private DBSurfaceGeometry surfaceGeometryImporter;
	private DBUndergroundThematicSurface thematicSurfaceImporter;
	private DBBuildingInstallation buildingInstallationImporter;
	private DBRoom roomImporter;
	private DBAddress addressImporter;
	private DBOtherGeometry otherGeometryImporter;

	private int batchCounter;
	private int nullGeometryType;
	private String nullGeometryTypeName;	

	public DBUnderground(Connection batchConn, DBImporterManager dbImporterManager) throws SQLException {
		this.batchConn = batchConn;
		this.dbImporterManager = dbImporterManager;

		init();
	}

	private void init() throws SQLException {
		nullGeometryType = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryType();
		nullGeometryTypeName = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryTypeName();

		StringBuilder stmt = new StringBuilder()
		.append("insert into UNDERGROUND (ID, UNDERGROUND_PARENT_ID, UNDERGROUND_ROOT_ID, CLASS, CLASS_CODESPACE, FUNCTION, FUNCTION_CODESPACE, USAGE, USAGE_CODESPACE, LEVEL, LEVEL_CODESPACE,")
		.append("START_HEIGHT, START_HEIGHT_UNIT, CEILING_HEIGHT , CEILING_HEIGHT_UNIT, AREA, AREA_UNIT,")
		.append("LOD1_TERRAIN_INTERSECTION, LOD2_TERRAIN_INTERSECTION, LOD3_TERRAIN_INTERSECTION, LOD4_TERRAIN_INTERSECTION, LOD2_MULTI_CURVE, LOD3_MULTI_CURVE, LOD4_MULTI_CURVE, ")
		.append("LOD0_FOOTPRINT_ID, LOD0_ROOFPRINT_ID, LOD1_MULTI_SURFACE_ID, LOD2_MULTI_SURFACE_ID, LOD3_MULTI_SURFACE_ID, LOD4_MULTI_SURFACE_ID, ")
		.append("LOD1_SOLID_ID, LOD2_SOLID_ID, LOD3_SOLID_ID, LOD4_SOLID_ID) values ")
		.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		psUnderground = batchConn.prepareStatement(stmt.toString());

		surfaceGeometryImporter = (DBSurfaceGeometry)dbImporterManager.getDBImporter(DBImporterEnum.SURFACE_GEOMETRY);
		cityObjectImporter = (DBCityObject)dbImporterManager.getDBImporter(DBImporterEnum.CITYOBJECT);
		thematicSurfaceImporter = (DBUndergroundThematicSurface)dbImporterManager.getDBImporter(DBImporterEnum.UNDERGROUND_THEMATIC_SURFACE);
		buildingInstallationImporter = (DBBuildingInstallation)dbImporterManager.getDBImporter(DBImporterEnum.BUILDING_INSTALLATION);
		roomImporter = (DBRoom)dbImporterManager.getDBImporter(DBImporterEnum.ROOM);
		addressImporter = (DBAddress)dbImporterManager.getDBImporter(DBImporterEnum.ADDRESS);
		otherGeometryImporter = (DBOtherGeometry)dbImporterManager.getDBImporter(DBImporterEnum.OTHER_GEOMETRY);
	}

	public long insert(AbstractUnderground underground) throws SQLException {
		long undergroundId = dbImporterManager.getDBId(DBSequencerEnum.CITYOBJECT_ID_SEQ);
		boolean success = false;

		if (undergroundId != 0)
			success = insert(underground, undergroundId, 0, undergroundId);

		if (success)
			return undergroundId;
		else
			return 0;
	}

	public boolean insert(AbstractUnderground underground,
			long undergroundId,
			long parentId,
			long rootId) throws SQLException {
		String origGmlId = underground.getId();

		// CityObject
		long cityObjectId = cityObjectImporter.insert(underground, undergroundId, parentId == 0);
		if (cityObjectId == 0)
			return false;

		// Underground
		// ID
		psUnderground.setLong(1, undergroundId);

		// UNDERGROUND_PARENT_ID
		if (parentId != 0)
			psUnderground.setLong(2, parentId);
		else
			psUnderground.setNull(2, Types.NULL);

		// UNDERGROUND_ROOT_ID
		psUnderground.setLong(3, rootId);

		// ug:Underground
		if (underground.isSetClazz() && underground.getClazz().isSetValue()) {
			psUnderground.setString(4, underground.getClazz().getValue());
			psUnderground.setString(5, underground.getClazz().getCodeSpace());
		} else {
			psUnderground.setNull(4, Types.VARCHAR);
			psUnderground.setNull(5, Types.VARCHAR);
		}

		// ug:function
		if (underground.isSetFunction()) {
			String[] function = Util.codeList2string(underground.getFunction());
			psUnderground.setString(6, function[0]);
			psUnderground.setString(7, function[1]);
		} else {
			psUnderground.setNull(6, Types.VARCHAR);
			psUnderground.setNull(7, Types.VARCHAR);
		}

		// ug:usage
		if (underground.isSetUsage()) {
			String[] usage = Util.codeList2string(underground.getUsage());
			psUnderground.setString(8, usage[0]);
			psUnderground.setString(9, usage[1]);
		} else {
			psUnderground.setNull(8, Types.VARCHAR);
			psUnderground.setNull(9, Types.VARCHAR);
		}

		// ug:level
		if (underground.isSetLevel() && underground.getLevel().isSetValue()) {
			psUnderground.setString(10, underground.getLevel().getValue());
			psUnderground.setString(11, underground.getLevel().getCodeSpace());
		} else {
			psUnderground.setNull(10, Types.VARCHAR);
			psUnderground.setNull(11, Types.VARCHAR);
		}

		// start:startHeight
		if (underground.isSetStartHeight() && underground.getStartHeight().isSetValue()) {
			psUnderground.setDouble(12, underground.getStartHeight().getValue());
			psUnderground.setString(13, underground.getStartHeight().getUom());
		} else {
			psUnderground.setNull(12, Types.DOUBLE);
			psUnderground.setNull(13, Types.VARCHAR);
		}

		// bldg:ceilingHeight
		if (underground.isSetCeilingHeight() && underground.getCeilingHeight().isSetValue()) {
			psUnderground.setDouble(14, underground.getCeilingHeight().getValue());
			psUnderground.setString(15, underground.getCeilingHeight().getUom());
		} else {
			psUnderground.setNull(14, Types.DOUBLE);
			psUnderground.setNull(15, Types.VARCHAR);
		}

		// bldg:area
		if (underground.isSetArea() && underground.getArea().isSetValue()) {
			psUnderground.setDouble(16, underground.getCeilingHeight().getValue());
			psUnderground.setString(17, underground.getCeilingHeight().getUom());
		} else {
			psUnderground.setNull(16, Types.DOUBLE);
			psUnderground.setNull(17, Types.VARCHAR);
		}

		// Geometry
		// lodXTerrainIntersectionCurve
		for (int i = 0; i < 4; i++) {
			MultiCurveProperty multiCurveProperty = null;
			GeometryObject multiLine = null;

			switch (i) {
			case 0:
				multiCurveProperty = underground.getLod1TerrainIntersection();
				break;
			case 1:
				multiCurveProperty = underground.getLod2TerrainIntersection();
				break;
			case 2:
				multiCurveProperty = underground.getLod3TerrainIntersection();
				break;
			case 3:
				multiCurveProperty = underground.getLod4TerrainIntersection();
				break;
			}

			if (multiCurveProperty != null) {
				multiLine = otherGeometryImporter.getMultiCurve(multiCurveProperty);
				multiCurveProperty.unsetMultiCurve();
			}

			if (multiLine != null) {
				Object multiLineObj = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(multiLine, batchConn);
				psUnderground.setObject(18 + i, multiLineObj);
			} else
				psUnderground.setNull(18 + i, nullGeometryType, nullGeometryTypeName);
		}

		// lodXMultiCurve
		for (int i = 0; i < 3; i++) {
			MultiCurveProperty multiCurveProperty = null;
			GeometryObject multiLine = null;

			switch (i) {
			case 0:
				multiCurveProperty = underground.getLod2MultiCurve();
				break;
			case 1:
				multiCurveProperty = underground.getLod3MultiCurve();
				break;
			case 2:
				multiCurveProperty = underground.getLod4MultiCurve();
				break;
			}

			if (multiCurveProperty != null) {
				multiLine = otherGeometryImporter.getMultiCurve(multiCurveProperty);
				multiCurveProperty.unsetMultiCurve();
			}

			if (multiLine != null) {
				Object multiLineObj = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(multiLine, batchConn);
				psUnderground.setObject(22 + i, multiLineObj);
			} else
				psUnderground.setNull(22 + i, nullGeometryType, nullGeometryTypeName);
		}

		// lod0FootPrint and lod0RoofEdge
		for (int i = 0; i < 2; i++) {
			MultiSurfaceProperty multiSurfaceProperty = null;
			long multiSurfaceId = 0;

			switch (i) {
			case 0:
				multiSurfaceProperty = underground.getLod0FootPrint();
				break;
			case 1:
				multiSurfaceProperty = underground.getLod0RoofEdge();
				break;			
			}

			if (multiSurfaceProperty != null) {
				if (multiSurfaceProperty.isSetMultiSurface()) {
					multiSurfaceId = surfaceGeometryImporter.insert(multiSurfaceProperty.getMultiSurface(), undergroundId);
					multiSurfaceProperty.unsetMultiSurface();
				} else {
					// xlink
					String href = multiSurfaceProperty.getHref();

					if (href != null && href.length() != 0) {
						dbImporterManager.propagateXlink(new DBXlinkSurfaceGeometry(
								href, 
								undergroundId, 
								TableEnum.UNDERGROUND, 
								i == 0 ? "LOD0_FOOTPRINT_ID" : "LOD0_ROOFPRINT_ID"));
					}
				}
			}

			if (multiSurfaceId != 0)
				psUnderground.setLong(25 + i, multiSurfaceId);
			else
				psUnderground.setNull(25 + i, Types.NULL);
		}

		// lodXMultiSurface
		for (int i = 0; i < 4; i++) {
			MultiSurfaceProperty multiSurfaceProperty = null;
			long multiGeometryId = 0;

			switch (i) {
			case 0:
				multiSurfaceProperty = underground.getLod1MultiSurface();
				break;
			case 1:
				multiSurfaceProperty = underground.getLod2MultiSurface();
				break;
			case 2:
				multiSurfaceProperty = underground.getLod3MultiSurface();
				break;
			case 3:
				multiSurfaceProperty = underground.getLod4MultiSurface();
				break;
			}

			if (multiSurfaceProperty != null) {
				if (multiSurfaceProperty.isSetMultiSurface()) {
					multiGeometryId = surfaceGeometryImporter.insert(multiSurfaceProperty.getMultiSurface(), undergroundId);
					multiSurfaceProperty.unsetMultiSurface();
				} else {
					// xlink
					String href = multiSurfaceProperty.getHref();

					if (href != null && href.length() != 0) {
						dbImporterManager.propagateXlink(new DBXlinkSurfaceGeometry(
								href, 
								undergroundId, 
								TableEnum.UNDERGROUND, 
								"LOD" + (i + 1) + "_MULTI_SURFACE_ID"));
					}
				}
			}

			if (multiGeometryId != 0)
				psUnderground.setLong(27 + i, multiGeometryId);
			else
				psUnderground.setNull(27 + i, Types.NULL);
		}

		// lodXSolid
		for (int i = 0; i < 4; i++) {
			SolidProperty solidProperty = null;
			long solidGeometryId = 0;

			switch (i) {
			case 0:
				solidProperty = underground.getLod1Solid();
				break;
			case 1:
				solidProperty = underground.getLod2Solid();
				break;
			case 2:
				solidProperty = underground.getLod3Solid();
				break;
			case 3:
				solidProperty = underground.getLod4Solid();
				break;
			}

			if (solidProperty != null) {
				if (solidProperty.isSetSolid()) {
					solidGeometryId = surfaceGeometryImporter.insert(solidProperty.getSolid(), undergroundId);
					solidProperty.unsetSolid();
				} else {
					// xlink
					String href = solidProperty.getHref();
					if (href != null && href.length() != 0) {
						dbImporterManager.propagateXlink(new DBXlinkSurfaceGeometry(
								href, 
								undergroundId, 
								TableEnum.UNDERGROUND, 
								"LOD" + (i + 1) + "_SOLID_ID"));
					}
				}
			}

			if (solidGeometryId != 0)
				psUnderground.setLong(31 + i, solidGeometryId);
			else
				psUnderground.setNull(31 + i, Types.NULL);
		}

		psUnderground.addBatch();
		if (++batchCounter == dbImporterManager.getDatabaseAdapter().getMaxBatchSize())
			dbImporterManager.executeBatch(DBImporterEnum.UNDERGROUND);

		// BoundarySurfaces
		if (underground.isSetBoundedBySurface()) {
			for (BoundarySurfaceProperty boundarySurfaceProperty : underground.getBoundedBySurface()) {
				AbstractBoundarySurface boundarySurface = boundarySurfaceProperty.getBoundarySurface();

				if (boundarySurface != null) {
					String gmlId = boundarySurface.getId();
					long id = thematicSurfaceImporter.insert(boundarySurface, underground.getCityGMLClass(), undergroundId);

					if (id == 0) {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								underground.getCityGMLClass(), 
								origGmlId));
						msg.append(": Failed to write ");
						msg.append(Util.getFeatureSignature(
								boundarySurface.getCityGMLClass(), 
								gmlId));

						LOG.error(msg.toString());
					}

					// free memory of nested feature
					boundarySurfaceProperty.unsetBoundarySurface();
				} else {
					// xlink
					String href = boundarySurfaceProperty.getHref();

					if (href != null && href.length() != 0) {
						LOG.error("XLink reference '" + href + "' to " + CityGMLClass.ABSTRACT_UNDERGROUND_BOUNDARY_SURFACE + " feature is not supported.");
					}
				}
			}
		}

		// BuildingPart
		if (underground.isSetConsistsOfUndergroundPart()) {
			for (UndergroundPartProperty undergroundPartProperty : underground.getConsistsOfUndergroundPart()) {
				UndergroundPart undergroundPart = undergroundPartProperty.getUndergroundPart();

				if (undergroundPart != null) {
					long id = dbImporterManager.getDBId(DBSequencerEnum.CITYOBJECT_ID_SEQ);

					if (id != 0)
						insert(undergroundPart, id, undergroundId, rootId);
					else {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								underground.getCityGMLClass(), 
								origGmlId));
						msg.append(": Failed to write ");
						msg.append(Util.getFeatureSignature(
								CityGMLClass.UNDERGROUND_PART, 
								undergroundPart.getId()));

						LOG.error(msg.toString());
					}

					// free memory of nested feature
					undergroundPartProperty.unsetUndergroundPart();
				} else {
					// xlink
					String href = undergroundPartProperty.getHref();

					if (href != null && href.length() != 0) {
						LOG.error("XLink reference '" + href + "' to " + CityGMLClass.UNDERGROUND_PART + " feature is not supported.");
					}
				}
			}
		}
		
		// insert local appearance
		cityObjectImporter.insertAppearance(underground, undergroundId);

		return true;
	}

	@Override
	public void executeBatch() throws SQLException {
		psUnderground.executeBatch();
		batchCounter = 0;
	}

	@Override
	public void close() throws SQLException {
		psUnderground.close();
	}

	@Override
	public DBImporterEnum getDBImporterType() {
		return DBImporterEnum.UNDERGROUND;
	}

}
