﻿/*
    Copyright (C) 2006-2018. Aardvark Platform Team. http://github.com/aardvark-platform.
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.
    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
using Aardvark.Data.Points;

namespace Aardvark.Geometry.Points
{
    /// <summary></summary>
    public static class PointCloudFormatExtensions
    {
        /// <summary></summary>
        public static PointSet ImportFile(this PointCloudFileFormat self, string filename, ImportConfig config)
            => PointCloud.Chunks(self.ParseFile(filename, config.ParseConfig), config);
    }
}
