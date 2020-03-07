// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once
#include "gis/cuda/conversion/conversions.h"
#include "gis/cuda/mock/arrow/api.h"

namespace zilliz {
namespace gis {
namespace cuda {

namespace internal {
struct WkbArrowContext {
  char* values;
  int* offsets;
  int size;

 public:
  DEVICE_RUNNABLE inline char* get_wkb_ptr(int index) { return values + offsets[index]; }
  DEVICE_RUNNABLE inline const char* get_wkb_ptr(int index) const {
    return values + offsets[index];
  }
  DEVICE_RUNNABLE inline int null_counts() const { return 0 * size; }
};

GeometryVector ArrowWkbToGeometryVectorImpl(const WkbArrowContext& input);

// return size of total data length in bytes
void ToArrowWkbFillOffsets(ConstGpuContext& input, WkbArrowContext& output,
                           int* value_length);

void ToArrowWkbFillValues(ConstGpuContext& input, WkbArrowContext& output);

}  // namespace internal

}  // namespace cuda
}  // namespace gis
}  // namespace zilliz