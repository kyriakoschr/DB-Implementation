typedef struct Record {
	int id;
	char name[15];
	char surname[20];
	char status[10];
	char dateOfBirth[11];
	int salary;
} Record;

typedef struct ColumnFds {
	char *columnName;
	int fd;
} ColumnFds;

typedef struct HeaderInfo {
	ColumnFds *column_fds;		// Πίνακας με τους FDs για όλα τα column files
} HeaderInfo;

/*
 * Η συνάρτηση CS_CreateFiles χρησιμοποιείται για τη δημιουργία και 
 * κατάλληλη αρχικοποίηση άδειων αρχείων, ένα για κάθε πεδίο που 
 * προσδιορίζεται από τον πίνακα fieldNames (και πρόκειται ουσιαστικά 
 * για τα πεδία της δομής Record). Τα αρχεία θα δημιουργούνται στο 
 * φάκελο με μονοπάτι dbDirectory. Σε περίπτωση που εκτελεστεί 
 * επιτυχώς η συνάρτηση, επιστρέφεται 0, αλλιώς ‐1.
 */
int CS_CreateFiles(char **fieldNames, char *dbDirectory);

/*
 * Η συνάρτηση CS_OpenFile ανοίγει το αρχείο “header_info” που βρήσκεται 
 * στο dbDirectory και επιστρέφει στο struct HeaderInfo τον αναγνωριστικό 
 * αριθμό ανοίγματος κάθε αρχείου, όπως αυτός επιστράφηκε από το επίπεδο 
 * διαχείρισης μπλοκ. Η συνάρτηση επιστρέφει ‐1 σε περίπτωση σφάλματος 0 αλλιώς.
 */
int CS_OpenFile(HeaderInfo* header_info, char *dbDirectory);

/*
 * Η συνάρτηση CS_CloseFile βρίσκει τα fileDesc των αρχείων που είναι
 * ανοικτά και τα κλείνει. Σε περίπτωση που εκτελεστεί επιτυχώς, 
 * επιστρέφεται 0, ενώ σε διαφορετική περίπτωση ‐1.
 */
int CS_CloseFile(HeaderInfo* header_info);


/*
 * Η συνάρτηση CS_InsertEntry χρησιμοποιείται για την εισαγωγή μίας εγγραφής 
 * στα αρχεία column‐store. Η πληροφορία για τα αρχεία CS διαβάζεται από τη 
 * δομή HeaderInfo, ενώ η εγγραφή προς εισαγωγή προσδιορίζεται από τη δομή 
 * Record. Η τιμή του κάθε πεδίου προστίθεται στο τέλος του αντίστοιχου αρχείου, 
 * μετά την τρέχουσα τελευταία τιμή που υπήρχε. Σε περίπτωση που εκτελεστεί 
 * επιτυχώς, επιστρέφεται 0, ενώ σε διαφορετική περίπτωση ‐1.
 */
int CS_InsertEntry(HeaderInfo* header_info, Record record);

/*
 * Η συνάρτηση αυτή χρησιμοποιείται για την εκτύπωση όλων των εγγραφών οι οποίες
 * έχουν τιμή στο πεδίο με όνομα fieldName ίση με value. Οι αναγνωριστικοί αριθμοί
 * ανοίγματος αρχείων δίνονται στη δομή HeaderInfo.  Η  παράμετρος  fieldName  
 * μπορεί  να  πάρει  για  τιμή  κάποιο  από τα ονόματα των πεδίων της εγγραφής. 
 * Σε περίπτωση που η τιμή του fieldName και του value είναι ίση με NULL, να
 * εκτυπώνονται όλες οι εγγραφές. Να εκτυπώνεται επίσης το πλήθος των μπλοκ 
 * που διαβάστηκαν.
 */
void CS_GetAllEntries(HeaderInfo* header_info, 
                      char *fieldName, 
                      void *value,
                      char **fieldNames,
                      int n);
 


