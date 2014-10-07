#ifndef __PARDIS_CLIB_H__
	#define __PARDIS_CLIB_H__

unsigned long long timeval_subtract(struct timeval *result, struct timeval *t2, struct timeval *t1) {
   	int diff = (t2->tv_usec + 1000000 * t2->tv_sec) - (t1->tv_usec + 1000000 * t1->tv_sec);
	result->tv_sec = diff / 1000000;
	result->tv_usec = diff % 1000000;
    return ((result->tv_sec * 1000) + (result->tv_usec/1000));
}

char* ltoa(long num) {
	const int n = snprintf(NULL, 0, "%lu", num);
	char* buf = (char*)malloc((n+1)*sizeof(char));
	snprintf(buf, n+1, "%lu", num);
	return buf;
} 

#endif
