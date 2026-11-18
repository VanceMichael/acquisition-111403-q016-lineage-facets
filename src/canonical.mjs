// 稳定序列化：对象键排序后输出 JSON，使同内容的字节摘要与归档版本可复现。
export function canonicalize(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return '[' + value.map(canonicalize).join(',') + ']';
  const keys = Object.keys(value).sort();
  return '{' + keys.map(key => JSON.stringify(key) + ':' + canonicalize(value[key])).join(',') + '}';
}

export const canonicalBytes = value => Buffer.from(canonicalize(value), 'utf8');
