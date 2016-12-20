#ifndef __MMTS_UTILS_H
#define __MMTS_UTILS_H

#ifdef __SCO_VERSION__
// SCO Specific stuff
#define strcasestr stristr
#define OS	"SCO"
#else
#define OS	"LINUX"
// Non-SCO specific stuff
#endif

#include <rogxml.h>

enum PROTOCOL_VERSION { NO_PROTOCOL, EVO12 };

void mmts_TrimTrailing(char *szText);
void SetBaseDir(const char *szDir);
const char *GetBaseDir();
void SetEtcDir(const char *szDir);
const char *GetEtcDir();
const char *GetConfigFile();
void SetConfigFile(const char *szFile);

void SetUtilsEnvironment(const char *szEnv);
const char *TimeStamp(time_t t);
const char *HL7TimeStamp(time_t t);
int MkDir(const char *szDir);
const char *QualifyFile(const char *szName);
const char *SignalName(int n);
char *SkipSpaces(const char *t);
char *ReadLine(FILE *fd);
void pack_InitPack();
int pack_Pack(const char *data, int nLen);
const char *pack_GetPacked(int *pnLen);
void pack_InitUnpack();
int pack_Unpack(const char *data, int nLen);
const char *pack_GetUnpacked(int *pnLen);
int LogError(int err, const char *szFmt, ...);
const char *MessageDescription(const char *szInteractionId, const char **pszSection);
int MessageOption(const char *szInteractionId, const char *szTest);
int ProcessAlive(int nPid);

int config_FileSetString(const char *szFilename, const char *szName, const char *szValue);
int config_SetString(const char *szName, const char *szValue);
const char *config_FileGetString(const char *szFilename, const char *szName);
const char *config_GetString(const char *szName);
int config_GetInt(const char *szName, int nDef);
const char *config_GetFile(const char *szName, const char *szDef);
const char *config_GetDir(const char *szName, const char *szDef);
const char *config_EnvGetString(const char *szName);
int config_EnvGetInt(const char *szName, int nDef);
char config_EnvGetBool(const char *szName, char nDef);

void Log(const char *szFmt, ...);

int PeerPort(int bExt);
const char *PeerIp(char bExt);
int rog_MkDir(const char *fmt, ...);

time_t DecodeTimeStamp(const char *szDt);
int ValidateXml(rogxml *rx);
time_t config_LastChange(void);

#endif
