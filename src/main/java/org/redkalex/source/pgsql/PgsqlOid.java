/*
 * Licensed under the Apache License, Version 2.0  = the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redkalex.source.pgsql;

/**
 * Object identifiers, copied from org.postgresql.core.PgOid.
 *
 * @author zhangjx
 */
public interface PgsqlOid {

	int UNSPECIFIED = 0;

	int INT2 = 21;

	int INT2_ARRAY = 1005;

	int INT4 = 23;

	int INT4_ARRAY = 1007;

	int INT8 = 20;

	int INT8_ARRAY = 1016;

	int TEXT = 25;

	int TEXT_ARRAY = 1009;

	int FLOAT4 = 700;

	int FLOAT4_ARRAY = 1021;

	int FLOAT8 = 701;

	int FLOAT8_ARRAY = 1022;

	int BOOL = 16;

	int BOOL_ARRAY = 1000;

	int DATE = 1082;

	int DATE_ARRAY = 1182;

	int TIME = 1083;

	int TIME_ARRAY = 1183;

	int TIMETZ = 1266;

	int TIMETZ_ARRAY = 1270;

	int TIMESTAMP = 1114;

	int TIMESTAMP_ARRAY = 1115;

	int TIMESTAMPTZ = 1184;

	int TIMESTAMPTZ_ARRAY = 1185;

	int BYTEA = 17;

	int BYTEA_ARRAY = 1001;

	int VARCHAR = 1043;

	int VARCHAR_ARRAY = 1015;

	int OID = 26;

	int OID_ARRAY = 1028;

	int BPCHAR = 1042;

	int BPCHAR_ARRAY = 1014;

	int MONEY = 790;

	int MONEY_ARRAY = 791;

	int NAME = 19;

	int NAME_ARRAY = 1003;

	int BIT = 1560;

	int BIT_ARRAY = 1561;

	int VOID = 2278;

	int INTERVAL = 1186;

	int INTERVAL_ARRAY = 1187;

	int CHAR = 18;

	int CHAR_ARRAY = 1002;

	int VARBIT = 1562;

	int VARBIT_ARRAY = 1563;

	int UUID = 2950;

	int UUID_ARRAY = 2951;

	int XML = 142;

	int XML_ARRAY = 143;

	int POINT = 600;

	int BOX = 603;

	int JSON = 114; // Added

	int JSONB = 3802;

	int HSTORE = 33670;

	int NUMERIC = 1700; // false  Number

	int NUMERIC_ARRAY = 1231; // false  Number

	int UNKNOWN = 705; // false String

	int TS_VECTOR = 3614; // false String

	int TS_VECTOR_ARRAY = 3643; // false String

	int TS_QUERY = 3615; // false String

	int TS_QUERY_ARRAY = 3645; // false String
}
