package org.yardstickframework.ignite.util

import org.apache.ignite.scalar.scalar._

/**
 * Created by sany on 8/7/15.
 */
case class Twitter (
                     @ScalarCacheQuerySqlField
                     id:String,
                     @ScalarCacheQuerySqlField
                     tweet:String,
                     @ScalarCacheQuerySqlField
                     created_at:String,
                     @ScalarCacheQuerySqlField
                     Fcount:String,
                     @ScalarCacheQuerySqlField
                     Rcount:String,
                     @ScalarCacheQuerySqlField
                     Glocation:String,
                     @ScalarCacheQuerySqlField
                     country:String,
                     @ScalarCacheQuerySqlField
                     username:String
                     )

