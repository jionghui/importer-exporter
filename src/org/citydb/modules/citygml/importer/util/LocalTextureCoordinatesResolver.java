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
package org.citydb.modules.citygml.importer.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.citydb.api.geometry.GeometryObject;

public class LocalTextureCoordinatesResolver {
	private final HashMap<Long, SurfaceGeometryTarget> targets;
	private final HashMap<String, LinearRing> rings;
	private final HashSet<SurfaceGeometryTarget> localContext;
	private boolean isActive;

	public LocalTextureCoordinatesResolver() {
		targets = new HashMap<Long, SurfaceGeometryTarget>();
		rings = new HashMap<String, LinearRing>();
		localContext = new HashSet<SurfaceGeometryTarget>();
	}
	
	public void reset() {
		targets.clear();
		rings.clear();
		localContext.clear();
		isActive = false;
	}

	public boolean isActive() {
		return isActive;
	}

	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}

	public void registerLinearRing(String ringId, long surfaceGeometryId, boolean isReverse) {
		if (!isActive || rings.containsKey(ringId))
			return;

		SurfaceGeometryTarget target = targets.get(surfaceGeometryId);
		if (target == null) {
			target = new SurfaceGeometryTarget(surfaceGeometryId, isReverse);
			targets.put(surfaceGeometryId, target);
		}
		
		LinearRing ring = new LinearRing(target);
		rings.put(ringId, ring);

		// we rely that the linear rings are registered in the order they
		// appear in the parent surface geometry
		target.rings.add(ring);
	}
	
	public boolean setTextureCoordinates(String ringId, List<Double> texCoords) {
		LinearRing ring = rings.get(ringId);
		if (ring != null) {
			ring.texCoords = texCoords;
			localContext.add(ring.target);
			return true;
		}
		
		return false;
	}
	
	public HashSet<SurfaceGeometryTarget> getLocalContext() {
		return localContext;
	}
	
	public void clearLocalContext() {
		for (SurfaceGeometryTarget target : localContext)
			for (LinearRing ring : target.rings)
				ring.texCoords = null;
		
		localContext.clear();
	}
	
	public class SurfaceGeometryTarget {
		private final long surfaceGeometryId;
		private final boolean isReverse;
		private final List<LinearRing> rings;

		private SurfaceGeometryTarget(long surfaceGeometryId, boolean isReverse) {
			this.surfaceGeometryId = surfaceGeometryId;
			this.isReverse = isReverse;
			rings = new ArrayList<LinearRing>();
		}
		
		public boolean isComplete() {
			for (LinearRing ring : rings)
				if (ring.texCoords == null)
					return false;
			
			return true;
		}

		public GeometryObject compileTextureCoordinates() {
			double[][] coordinates = new double[rings.size()][];			
			
			for (int i = 0; i < rings.size(); i++) {
				LinearRing ring = rings.get(i);
				coordinates[i] = new double[ring.texCoords.size()];
				
				if (!isReverse) {
					for (int j = 0; j < ring.texCoords.size(); j++)
						coordinates[i][j] = ring.texCoords.get(j);
				} else {
					for (int j = ring.texCoords.size() - 2, k = 0; j >= 0; j -= 2) {
						coordinates[i][k++] = ring.texCoords.get(j);
						coordinates[i][k++] = ring.texCoords.get(j + 1);
					}
				}
			}
			
			return GeometryObject.createPolygon(coordinates, 2, 0);
		}
		
		public long getSurfaceGeometryId() {
			return surfaceGeometryId;
		}
	}
	
	private class LinearRing {
		private final SurfaceGeometryTarget target;
		private List<Double> texCoords;
		
		private LinearRing(SurfaceGeometryTarget target) {
			this.target = target;
		}
	}

}
