/* bpf_pin.h — tiny helper to pin BPF maps under a common bpffs directory
 *
 * Pinning lets external tools (such as kvs-top) open kvstore's live BPF maps
 * read-only without re-loading the BPF object.  All kvstore maps are pinned
 * under a single per-instance directory: KVS_PIN_DIR/<map_name>.
 */
#ifndef BPF_PIN_H
#define BPF_PIN_H

#define KVS_PIN_DIR "/sys/fs/bpf/kvstore"

#ifdef __cplusplus
extern "C" {
#endif

/* Ensure KVS_PIN_DIR exists.  Returns 0 on success, -1 on error. */
int  kvs_bpf_pin_dir_ensure(void);

/* Pin map fd at KVS_PIN_DIR/<name>.  If a stale pin exists it is unlinked
 * first.  Returns 0 on success, -1 on failure (errno set). */
int  kvs_bpf_pin_map(int map_fd, const char *name);

/* Remove pinned map KVS_PIN_DIR/<name> (no-op if it does not exist). */
void kvs_bpf_unpin_map(const char *name);

/* Open a previously-pinned map read-only.  Returns fd >= 0 on success, -1 on error. */
int  kvs_bpf_open_pinned(const char *name);

#ifdef __cplusplus
}
#endif

#endif /* BPF_PIN_H */
