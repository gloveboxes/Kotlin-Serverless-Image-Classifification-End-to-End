package com.glovebox.models

import com.microsoft.azure.storage.table.TableServiceEntity

class ImageProbabilityEntity: TableServiceEntity() {
    val boundingBox: String? = null
    val tagId: String = ""
    val tagName: String = ""
    var probability: Double = 0.0
    var count: Int = 0
}