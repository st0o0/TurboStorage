export default {
  extends: ['@commitlint/config-conventional'],
  rules: {
    'header-max-length': [1, 'always', 120],
    'body-max-line-length': [0, 'always'],
    'footer-max-line-length': [0, 'always'],
    'subject-case': [0, 'always'],
  },
};
