import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text
import hashlib
import json

class AuthorWorksConverter:
    """
    Converter untuk membuat relasi many-to-many antara Authors dan Works
    """
    
    def __init__(self, authors_csv: str, works_csv: str):
        """
        Inisialisasi dengan file CSV authors dan works
        
        Args:
            authors_csv: Path ke file authors.csv
            works_csv: Path ke file works.csv
        """
        self.authors_df = pd.read_csv(authors_csv)
        self.works_df = pd.read_csv(works_csv)
        
        # Bersihkan whitespace dari headers
        self.authors_df.columns = self.authors_df.columns.str.strip()
        self.works_df.columns = self.works_df.columns.str.strip()
        
        # Bersihkan data
        self._clean_data()
        
        # Buat ID unik untuk works jika belum ada
        if 'id_work' not in self.works_df.columns:
            self.works_df['id_work'] = self.works_df.apply(
                lambda row: self._generate_work_id(row), axis=1
            )
        
        # Hapus duplikat works berdasarkan id_work (keep first occurrence)
        works_before = len(self.works_df)
        self.works_df = self.works_df.drop_duplicates(subset=['id_work'], keep='first')
        works_after = len(self.works_df)
        if works_before != works_after:
            print(f"ℹ️  Removed {works_before - works_after} duplicate works (same DOI/title)")
    
    def _clean_data(self):
        """Bersihkan data dari karakter yang bisa menyebabkan error SQL"""
        # Clean authors dataframe
        for col in self.authors_df.columns:
            if self.authors_df[col].dtype == 'object':
                self.authors_df[col] = self.authors_df[col].apply(self._clean_text)
        
        # Clean works dataframe
        for col in self.works_df.columns:
            if self.works_df[col].dtype == 'object':
                self.works_df[col] = self.works_df[col].apply(self._clean_text)
    
    def _clean_text(self, text):
        """Bersihkan text dari karakter problematik"""
        if pd.isna(text):
            return None
        
        text = str(text)
        # Hapus null bytes yang bisa menyebabkan error
        text = text.replace('\x00', '')
        # Normalize whitespace
        text = ' '.join(text.split())
        # Truncate jika terlalu panjang (max 65535 untuk TEXT)
        if len(text) > 65000:
            text = text[:65000] + '...'
        
        return text if text else None
    
    def _generate_work_id(self, row) -> str:
        """Generate ID unik untuk work berdasarkan DOI atau hash dari title+authors"""
        if pd.notna(row.get('doi')) and row['doi']:
            return f"doi_{row['doi']}"
        else:
            # Hash dari title + authors
            content = f"{row.get('title', '')}_{row.get('authors', '')}"
            hash_id = hashlib.md5(content.encode()).hexdigest()[:16]
            return f"work_{hash_id}"
    
    def _create_junction_table(self) -> pd.DataFrame:
        """
        Buat tabel junction (author_works) dari relasi many-to-many
        
        Returns:
            DataFrame dengan kolom: id_author, id_work
        """
        junction_data = []
        
        for _, work in self.works_df.iterrows():
            authors_str = work.get('authors', '')
            author_query = work.get('author_query', '')
            
            # Parse authors (biasanya dalam format JSON array atau separated)
            author_names = self._parse_authors(authors_str)
            
            # Cari ID author yang match
            for author_name in author_names:
                # Cari di authors_df berdasarkan nama
                matches = self.authors_df[
                    self.authors_df['fullname'].str.contains(author_name, case=False, na=False) |
                    (self.authors_df['fullname'].str.lower() == author_name.lower())
                ]
                
                # Jika tidak ada match, coba cari dari author_query
                if len(matches) == 0 and pd.notna(author_query):
                    matches = self.authors_df[
                        self.authors_df['fullname'].str.contains(author_query, case=False, na=False)
                    ]
                
                for _, author in matches.iterrows():
                    junction_data.append({
                        'id_author': author['id_author'],
                        'id_work': work['id_work']
                    })
        
        return pd.DataFrame(junction_data).drop_duplicates()
    
    def _parse_authors(self, authors_str) -> list:
        """Parse string authors menjadi list nama"""
        if pd.isna(authors_str) or not authors_str:
            return []
        
        # Coba parse sebagai JSON
        try:
            authors_list = json.loads(authors_str)
            if isinstance(authors_list, list):
                return [a.get('given', '') + ' ' + a.get('family', '') 
                       if isinstance(a, dict) else str(a) 
                       for a in authors_list]
        except:
            pass
        
        # Fallback: split by common separators
        for sep in [';', ',', ' and ', '|']:
            if sep in authors_str:
                return [name.strip() for name in authors_str.split(sep)]
        
        return [authors_str.strip()]
    
    def to_sqlite(self, db_file: str, if_exists: str = 'replace'):
        """
        Insert data ke SQLite database dengan relasi many-to-many
        
        Args:
            db_file: Path ke file database SQLite
            if_exists: 'fail', 'replace', atau 'append'
        """
        conn = sqlite3.connect(db_file)
        
        # Insert tabel authors
        self.authors_df.to_sql('authors', conn, if_exists=if_exists, index=False)
        print(f"✓ Tabel 'authors' berhasil dibuat ({len(self.authors_df)} rows)")
        
        # Insert tabel works
        self.works_df.to_sql('works', conn, if_exists=if_exists, index=False)
        print(f"✓ Tabel 'works' berhasil dibuat ({len(self.works_df)} rows)")
        
        # Buat dan insert tabel junction
        junction_df = self._create_junction_table()
        junction_df.to_sql('author_works', conn, if_exists=if_exists, index=False)
        print(f"✓ Tabel 'author_works' (junction) berhasil dibuat ({len(junction_df)} rows)")
        
        # Buat indexes untuk performa
        cursor = conn.cursor()
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_author_works_author ON author_works(id_author)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_author_works_work ON author_works(id_work)')
        print("✓ Indexes berhasil dibuat")
        
        conn.commit()
        conn.close()
        print(f"\n✅ Database SQLite berhasil dibuat: {db_file}")
    
    def to_mysql(self, host: str, user: str, password: str, database: str, 
                 if_exists: str = 'replace'):
        """
        Insert data ke MySQL database dengan relasi many-to-many
        
        Args:
            host: MySQL host
            user: MySQL username
            password: MySQL password
            database: Nama database
            if_exists: 'fail', 'replace', atau 'append'
        """
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
        
        with engine.connect() as conn:
            # Drop tables jika exists untuk membuat ulang dengan constraints
            if if_exists == 'replace':
                print("Menghapus tabel lama jika ada...")
                conn.execute(text('SET FOREIGN_KEY_CHECKS = 0'))
                conn.execute(text('DROP TABLE IF EXISTS author_works'))
                conn.execute(text('DROP TABLE IF EXISTS works'))
                conn.execute(text('DROP TABLE IF EXISTS authors'))
                conn.execute(text('SET FOREIGN_KEY_CHECKS = 1'))
            
            # Buat tabel authors dengan PRIMARY KEY
            print("Membuat tabel authors...")
            authors_cols = self._get_mysql_columns(self.authors_df, 'id_author')
            create_authors = f"""
            CREATE TABLE authors (
                {', '.join(authors_cols)},
                PRIMARY KEY (id_author)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            conn.execute(text(create_authors))
            
            # Insert data authors
            print("Memasukkan data authors...")
            try:
                # Insert dengan batch untuk menghindari timeout
                batch_size = 100
                for i in range(0, len(self.authors_df), batch_size):
                    batch = self.authors_df.iloc[i:i+batch_size]
                    batch.to_sql('authors', conn, if_exists='append', index=False, method='multi')
                print(f"✓ Tabel 'authors' berhasil dibuat ({len(self.authors_df)} rows)")
            except Exception as e:
                print(f"❌ Error insert authors: {e}")
                raise
            
            # Buat tabel works dengan PRIMARY KEY
            print("Membuat tabel works...")
            works_cols = self._get_mysql_columns(self.works_df, 'id_work')
            create_works = f"""
            CREATE TABLE works (
                {', '.join(works_cols)},
                PRIMARY KEY (id_work)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            conn.execute(text(create_works))
            
            # Insert data works dengan error handling
            print("Memasukkan data works...")
            try:
                batch_size = 50  # Lebih kecil karena works punya banyak kolom
                for i in range(0, len(self.works_df), batch_size):
                    batch = self.works_df.iloc[i:i+batch_size]
                    batch.to_sql('works', conn, if_exists='append', index=False, method='multi')
                    if (i + batch_size) % 500 == 0:
                        print(f"  Progress: {min(i + batch_size, len(self.works_df))}/{len(self.works_df)} rows")
                print(f"✓ Tabel 'works' berhasil dibuat ({len(self.works_df)} rows)")
            except Exception as e:
                print(f"❌ Error insert works: {e}")
                print(f"  Error pada batch: {i}-{i+batch_size}")
                # Coba insert satu per satu untuk batch yang error
                print("  Mencoba insert satu per satu untuk menemukan row bermasalah...")
                for idx, row in batch.iterrows():
                    try:
                        pd.DataFrame([row]).to_sql('works', conn, if_exists='append', index=False)
                    except Exception as row_error:
                        print(f"  ⚠ Skip row {idx}: {str(row_error)[:100]}")
                        continue
            
            # Buat tabel junction dengan FOREIGN KEYS
            print("Membuat tabel author_works dengan foreign keys...")
            id_author_type = self._get_column_type(self.authors_df['id_author'])
            id_work_type = self._get_column_type(self.works_df['id_work'])
            
            create_junction = f"""
            CREATE TABLE author_works (
                id_author {id_author_type} NOT NULL,
                id_work {id_work_type} NOT NULL,
                PRIMARY KEY (id_author, id_work),
                FOREIGN KEY (id_author) REFERENCES authors(id_author) ON DELETE CASCADE ON UPDATE CASCADE,
                FOREIGN KEY (id_work) REFERENCES works(id_work) ON DELETE CASCADE ON UPDATE CASCADE,
                INDEX idx_author_works_author (id_author),
                INDEX idx_author_works_work (id_work)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            conn.execute(text(create_junction))
            
            # Insert data junction dengan error handling untuk foreign key
            print("Memasukkan relasi author-works...")
            junction_df = self._create_junction_table()
            
            # Filter junction_df untuk hanya include id_work yang ada di database
            valid_work_ids = set(self.works_df['id_work'].tolist())
            junction_before = len(junction_df)
            junction_df = junction_df[junction_df['id_work'].isin(valid_work_ids)]
            junction_after = len(junction_df)
            
            if junction_before != junction_after:
                print(f"ℹ️  Filtered out {junction_before - junction_after} relations with missing works")
            
            try:
                # Insert junction table
                batch_size = 1000
                for i in range(0, len(junction_df), batch_size):
                    batch = junction_df.iloc[i:i+batch_size]
                    batch.to_sql('author_works', conn, if_exists='append', index=False, method='multi')
                    if (i + batch_size) % 5000 == 0:
                        print(f"  Progress: {min(i + batch_size, len(junction_df))}/{len(junction_df)} relations")
                print(f"✓ Tabel 'author_works' (junction) berhasil dibuat ({len(junction_df)} rows)")
            except Exception as e:
                print(f"❌ Error insert junction: {e}")
                print("  Mencoba insert dengan handling foreign key constraint...")
                success_count = 0
                for idx, row in junction_df.iterrows():
                    try:
                        pd.DataFrame([row]).to_sql('author_works', conn, if_exists='append', index=False)
                        success_count += 1
                    except:
                        continue
                print(f"✓ Berhasil insert {success_count}/{len(junction_df)} relations")
            print("✓ Foreign keys dan indexes berhasil dibuat")
            
            conn.commit()
        
        print(f"\n✅ Database MySQL berhasil dibuat dengan relasi foreign key: {database}")
        self._print_foreign_keys_info()
    
    def _get_mysql_columns(self, df: pd.DataFrame, pk_column: str) -> list:
        """Generate kolom MySQL dengan tipe data yang sesuai"""
        columns = []
        for col, dtype in df.dtypes.items():
            col_type = self._get_column_type(df[col])
            if col == pk_column:
                columns.append(f"{col} {col_type} NOT NULL")
            else:
                columns.append(f"{col} {col_type}")
        return columns
    
    def _get_column_type(self, series: pd.Series) -> str:
        """Deteksi tipe kolom MySQL yang sesuai"""
        dtype = series.dtype
        
        if dtype == 'int64':
            # Cek range untuk menentukan INT vs BIGINT
            max_val = series.max() if len(series) > 0 else 0
            if max_val > 2147483647:
                return 'BIGINT'
            return 'INT'
        elif dtype == 'float64':
            return 'DOUBLE'
        else:
            # Untuk TEXT, cek panjang maksimal
            max_len = series.astype(str).str.len().max() if len(series) > 0 else 0
            if max_len <= 255:
                return 'VARCHAR(255)'
            elif max_len <= 65535:
                return 'TEXT'
            else:
                return 'MEDIUMTEXT'
    
    def _print_foreign_keys_info(self):
        """Print informasi tentang foreign keys yang dibuat"""
        print("\n" + "=" * 70)
        print("RELASI DATABASE (FOREIGN KEYS)")
        print("=" * 70)
        print("""
📋 Struktur Relasi:

1. Tabel: AUTHORS
   - Primary Key: id_author
   
2. Tabel: WORKS
   - Primary Key: id_work

3. Tabel: AUTHOR_WORKS (Junction Table)
   - Primary Key: (id_author, id_work) - Composite Key
   - Foreign Key: id_author → authors(id_author)
     └─ ON DELETE CASCADE (hapus author = hapus relasinya)
     └─ ON UPDATE CASCADE (update author id = update relasinya)
   - Foreign Key: id_work → works(id_work)
     └─ ON DELETE CASCADE (hapus work = hapus relasinya)
     └─ ON UPDATE CASCADE (update work id = update relasinya)
   - Index: idx_author_works_author
   - Index: idx_author_works_work

✅ Relasi many-to-many sudah terbentuk dengan constraint database!
        """)
    
    def to_postgresql(self, host: str, user: str, password: str, database: str,
                      if_exists: str = 'replace', port: int = 5432):
        """
        Insert data ke PostgreSQL database dengan relasi many-to-many
        
        Args:
            host: PostgreSQL host
            user: PostgreSQL username
            password: PostgreSQL password
            database: Nama database
            if_exists: 'fail', 'replace', atau 'append'
            port: PostgreSQL port
        """
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        
        with engine.connect() as conn:
            # Insert tabel authors
            self.authors_df.to_sql('authors', conn, if_exists=if_exists, index=False)
            print(f"✓ Tabel 'authors' berhasil dibuat ({len(self.authors_df)} rows)")
            
            # Insert tabel works
            self.works_df.to_sql('works', conn, if_exists=if_exists, index=False)
            print(f"✓ Tabel 'works' berhasil dibuat ({len(self.works_df)} rows)")
            
            # Buat dan insert tabel junction
            junction_df = self._create_junction_table()
            junction_df.to_sql('author_works', conn, if_exists=if_exists, index=False)
            print(f"✓ Tabel 'author_works' (junction) berhasil dibuat ({len(junction_df)} rows)")
            
            # Buat indexes
            conn.execute(text('CREATE INDEX idx_author_works_author ON author_works(id_author)'))
            conn.execute(text('CREATE INDEX idx_author_works_work ON author_works(id_work)'))
            print("✓ Indexes berhasil dibuat")
            
            conn.commit()
        
        print(f"\n✅ Database PostgreSQL berhasil dibuat: {database}")
    
    def generate_sql_file(self, output_file: str = 'database_schema.sql'):
        """
        Generate file SQL lengkap dengan semua tabel dan relasi
        
        Args:
            output_file: Nama file output SQL
        """
        with open(output_file, 'w', encoding='utf-8') as f:
            # CREATE TABLE authors
            f.write("-- Tabel Authors\n")
            f.write(self._generate_create_table_sql('authors', self.authors_df))
            f.write("\n\n")
            
            # CREATE TABLE works
            f.write("-- Tabel Works\n")
            f.write(self._generate_create_table_sql('works', self.works_df))
            f.write("\n\n")
            
            # CREATE TABLE junction
            f.write("-- Tabel Junction (Many-to-Many)\n")
            f.write("""CREATE TABLE IF NOT EXISTS author_works (
    id_author TEXT NOT NULL,
    id_work TEXT NOT NULL,
    PRIMARY KEY (id_author, id_work),
    FOREIGN KEY (id_author) REFERENCES authors(id_author),
    FOREIGN KEY (id_work) REFERENCES works(id_work)
);

CREATE INDEX idx_author_works_author ON author_works(id_author);
CREATE INDEX idx_author_works_work ON author_works(id_work);
""")
            f.write("\n\n")
            
            # INSERT authors
            f.write("-- Insert Authors\n")
            for _, row in self.authors_df.iterrows():
                f.write(self._generate_insert_sql('authors', row))
            f.write("\n\n")
            
            # INSERT works
            f.write("-- Insert Works\n")
            for _, row in self.works_df.iterrows():
                f.write(self._generate_insert_sql('works', row))
            f.write("\n\n")
            
            # INSERT junction
            f.write("-- Insert Author-Works Relations\n")
            junction_df = self._create_junction_table()
            for _, row in junction_df.iterrows():
                f.write(self._generate_insert_sql('author_works', row))
        
        print(f"✅ File SQL berhasil dibuat: {output_file}")
        print(f"   - Authors: {len(self.authors_df)} rows")
        print(f"   - Works: {len(self.works_df)} rows")
        print(f"   - Relations: {len(junction_df)} rows")
    
    def _generate_create_table_sql(self, table_name: str, df: pd.DataFrame) -> str:
        """Generate CREATE TABLE statement"""
        columns = []
        for col, dtype in df.dtypes.items():
            if dtype == 'int64':
                sql_type = 'INTEGER'
            elif dtype == 'float64':
                sql_type = 'REAL'
            else:
                sql_type = 'TEXT'
            
            # Primary key
            if col.startswith('id_'):
                columns.append(f"    {col} {sql_type} PRIMARY KEY")
            else:
                columns.append(f"    {col} {sql_type}")
        
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n" + ',\n'.join(columns) + "\n);"
    
    def _generate_insert_sql(self, table_name: str, row: pd.Series) -> str:
        """Generate INSERT statement untuk satu row"""
        columns = ', '.join(row.index)
        values = ', '.join([self._format_value(val) for val in row])
        return f"INSERT INTO {table_name} ({columns}) VALUES ({values});\n"
    
    def _format_value(self, val) -> str:
        """Format nilai untuk SQL"""
        if pd.isna(val):
            return 'NULL'
        elif isinstance(val, str):
            # Escape single quotes
            escaped = val.replace("'", "''").replace('\n', ' ').replace('\r', '')
            return f"'{escaped}'"
        else:
            return str(val)
    
    def print_schema_info(self):
        """Print informasi schema dan relasi"""
        print("=" * 70)
        print("DATABASE SCHEMA INFORMATION")
        print("=" * 70)
        print("\n📊 Tabel: AUTHORS")
        print(f"   Total records: {len(self.authors_df)}")
        print(f"   Kolom: {', '.join(self.authors_df.columns[:5])}...")
        
        print("\n📚 Tabel: WORKS")
        print(f"   Total records: {len(self.works_df)}")
        print(f"   Kolom: {', '.join(self.works_df.columns[:5])}...")
        
        junction_df = self._create_junction_table()
        print("\n🔗 Tabel: AUTHOR_WORKS (Junction Table)")
        print(f"   Total relations: {len(junction_df)}")
        print(f"   Kolom: id_author, id_work")
        
        print("\n📈 STATISTIK RELASI:")
        print(f"   Rata-rata author per work: {len(junction_df) / len(self.works_df):.2f}")
        print(f"   Rata-rata work per author: {len(junction_df) / len(self.authors_df):.2f}")
        print("=" * 70)


# Contoh penggunaan
if __name__ == "__main__":
    
    # 1. Generate file SQL dari CSV
    print("=== Contoh 1: Generate SQL File ===")
    authorPath = "./author/openalex/authors_cleaned.csv"
    worksPath = "./jurnal/crossref/crossref_works_full.csv"
    MYSQL_CONFIG = {
        'host': 'localhost',     
        'user': 'root',          
        'password': '',    
        'database': 'kp-penelitian-dosen', 
        'if_exists': 'replace' 
    }
    converter = AuthorWorksConverter(authorPath, worksPath)
    converter.print_schema_info()
    print("\n=== Insert ke MySQL ===")
    try:
        converter.to_mysql(**MYSQL_CONFIG)
        print("\n🎉 Sukses! Database sudah siap digunakan.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    
